package com.databricks.labs.gbx.rasterx.ds

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.util.NodeFileCacheProbe.{entries, garbage, withNetCDFCopy, withScratchDir}
import com.databricks.labs.gbx.util.{HadoopUtils, NodeCacheRetentionOff, NodeFileManager, NodeFilePathUtil}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, empty}

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._

// Idle retention is off, so a released node-file copy is gone at once.
class GDAL_DataSourceTest extends PlanTest with SilentSparkSession with NodeCacheRetentionOff {

    test("GDAL Data Source must read tif files") {
        import com.databricks.labs.gbx.rasterx.functions._
        rasterx.functions.register(spark)
        val sp = spark
        import sp.implicits._

        val tifPath = this.getClass.getResource("/modis/").toString

        val res = spark.read
            .format("gdal")
            .option("sizeInMB", "1")
            .load(tifPath)
            .limit(10)
            .select(
              rst_avg(col("tile")).alias("avg")
            )
            .as[Array[Double]]
            .collect()

        res.foreach(arr => arr.foreach(v => v should be >= 0.0))

    }

    test("GDAL reader keeps an all-NoData file as one row (parity with raster_gbx)") {
        rasterx.functions.register(spark)
        GDALManager.init(ExpressionConfig(spark))
        val dir = Files.createTempDirectory("gdal_all_nodata_")
        val tif = dir.resolve("all_nodata.tif").toString
        try {
            // 256x256 uint16, nodata=0, every pixel 0: the whole raster is NoData.
            val ds = GDALManager.gtiffDriver().Create(tif, 256, 256, 1, gdalconstConstants.GDT_UInt16)
            ds.SetGeoTransform(Array(500000.0, 10.0, 0.0, 4510000.0, 0.0, -10.0))
            val band = ds.GetRasterBand(1)
            band.SetNoDataValue(0.0)
            band.WriteRaster(0, 0, 256, 256, new Array[Short](256 * 256))
            ds.FlushCache()
            ds.delete()

            // Default (one tile per file) and a tiled read must both yield the NoData tile(s),
            // never an empty result.
            spark.read.format("gdal").load(dir.toString).count() shouldBe 1L
            spark.read.format("gdal").option("sizeInMB", "1").load(dir.toString).count() shouldBe 1L
        } finally {
            Files.list(dir).toList.asScala.foreach(Files.deleteIfExists)
            Files.deleteIfExists(dir)
        }
    }

    test("GDAL reader clipCrs option populates the v2 tile.clip_crs (parity with light)") {
        rasterx.functions.register(spark)
        val tifPath = this.getClass.getResource("/modis/").toString

        // clipCrs given -> tile.clip_crs is the canonical CRS string (incl. ESRI).
        val withOpt = spark.read
            .format("gdal")
            .option("sizeInMB", "1")
            .option("clipCrs", "ESRI:54008")
            .load(tifPath)
            .limit(1)
            .select(col("tile.clip_crs").alias("cc"))
            .collect()
        withOpt.length should be > 0
        withOpt.foreach(r => r.getString(0) should be("ESRI:54008"))

        // No clipCrs option -> clip_crs stays null (never errors).
        val noOpt = spark.read
            .format("gdal")
            .option("sizeInMB", "1")
            .load(tifPath)
            .limit(1)
            .select(col("tile.clip_crs").alias("cc"))
            .collect()
        noOpt.foreach(r => r.isNullAt(0) should be(true))
    }

    test("an invalid retention in the session conf fails a gdal read's planning and names the conf") {
        val modis = this.getClass.getResource("/modis/").toString
        withSessionRetention("soon") {
            val e = intercept[Exception](spark.read.format("gdal").load(modis).count())
            withClue(s"the error chain ${messageChain(e)}: ") {
                messageChain(e).exists(_.contains(NodeFileManager.RetentionKey)) shouldBe true
            }
        }
    }

    // The reader's one hold on a staged source is released by the source Dataset (GetFileList gives the
    // local copy), so an outside holder of the same remote keeps its copy. A `file:` path is non-local.
    test("N2 a gdal read of a shared .nc releases only its own hold") {
        rasterx.functions.register(spark)
        withNetCDFCopy { (dir, nc) =>
            val hconf = ExpressionConfig(spark).hConf
            NodeFileManager.init(hconf)
            val path = s"file:${dir.toAbsolutePath}"
            // The remote exactly as the gdal planner names it (listAllHadoopFiles), so the holder and the read
            // share one cache entry.
            val remote = HadoopUtils.listAllHadoopFiles(path, hconf, "").filter(_.endsWith(nc.getFileName.toString)).head
            val before = entries()
            val held = Paths.get(NodeFileManager.readRemote(remote))
            try {
                spark.read.format("gdal").load(path).count() should be > 0L
                withClue(s"the outside holder's copy $held after the read: ") { Files.isRegularFile(held) shouldBe true }
            } finally NodeFileManager.releaseLocal(held.toString)
            withClue(s"generation dir ${held.getParent} after the holder released: ") {
                Files.exists(held.getParent) shouldBe false
            }
            withClue("node-file cache entries left by the read: ") { entries().diff(before) shouldBe Set.empty[String] }
        }
    }

    test("RasterDriver.read of a staged file GDAL cannot open frees the copy it took") {
        GDALManager.init(ExpressionConfig(spark))
        withScratchDir("gbx_gdal_bad_") { dir =>
            val bad = dir.resolve("bad.tif")
            Files.write(bad, garbage)
            NodeFileManager.init(ExpressionConfig(spark).hConf)
            val before = entries()
            intercept[RuntimeException](RasterDriver.read(s"file:${bad.toAbsolutePath}", Map.empty))
            withClue("node-file cache entries left by the failed open: ") { entries().diff(before) shouldBe Set.empty[String] }
        }
    }

    // A Dataset opened on a /vsizip/ (or subdataset) form of a staged copy lists that form, not the copy's path,
    // so releaseDataset could never give the copy's hold back: such a remote read is refused before it stages.
    test("RasterDriver.read refuses isZip / isSubdataset on a remote path and stages nothing") {
        GDALManager.init(ExpressionConfig(spark))
        withScratchDir("gbx_gdal_zip_") { dir =>
            val z = dir.resolve("x.zip")
            Files.write(z, garbage)
            NodeFileManager.init(ExpressionConfig(spark))
            val remote = s"file:${z.toAbsolutePath}"
            val before = entries()
            for (opt <- Seq("isZip", "isSubdataset")) {
                val e = intercept[IllegalArgumentException](RasterDriver.read(remote, Map(opt -> "true")))
                withClue(s"the error for $opt, ${e.getMessage}: ") { assert(e.getMessage.contains(remote)) }
            }
            withClue(s"copies made for $remote: ") { NodeFilePathUtil.copyCount(remote) shouldBe 0 }
            withClue("node-file cache entries left: ") { entries().diff(before) shouldBe Set.empty[String] }
        }
    }

}
