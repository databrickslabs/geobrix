package com.databricks.labs.gbx.rasterx.ds

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.ListHasAsScala
import org.apache.spark.sql.functions.{concat, lit, monotonically_increasing_id}

class GDAL_DataSourceTest extends PlanTest with SilentSparkSession {

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

    test("GDAL Data Source must write valid tifs for rows") {
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df = spark.read
            .format("gdal")
            .option("sizeInMB", "1")
            .load(tifPath)

        df.write
            .format("gdal")
            .option("ext", "tif")
            .mode("append")
            .save("/tmp/gdal_test_out")

        val outPath = Paths.get("/tmp/gdal_test_out")

        val testFile = Files.list(outPath).filter(p => !p.toString.contains(".crc")).limit(1).toList.get(0)

        val ds = gdal.Open(testFile.toString)
        ds.GetRasterBand(1).AsMDArray().GetStatistics().getValid_count should be >= 0L

        val dss = Files.list(outPath).filter(p => !p.toString.contains(".crc")).toList.asScala.map(p => gdal.Open(p.toString)).toList
//        dss.foreach { ds =>
//            println("_".repeat(64))
//            RasterDebuger.printColorGridDenseTruecolor(ds, 128, 128)
//        }
//  RasterDebuger.printColorGridDenseTruecolor(dss.head, 128, 128)

//        while (true) {}

        Files.list(outPath).toList.asScala.foreach(Files.deleteIfExists)
        Files.deleteIfExists(outPath)

        //        while (true) {}

    }

    test("GDAL RowWriter default filename is digit-leading (regression: Hadoop hidden-file filter)") {
        // Regression for signed-hash bug: MurmurHash3.seqHash returns a signed Int, so
        // the old .toString.replace("-","_") produced "_"-leading names for ~half of all
        // tiles.  Hadoop's hidden-file filter silently skips files whose name starts with
        // "_", "-", or ".", so customers lost roughly half their heavy-writer output.
        // Fix (commit 666ca77d): Integer.toUnsignedString() — always digit-leading.
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString
        val outDir = Files.createTempDirectory("gdal_default_names_out_").toString

        try {
            spark.read
                .format("gdal")
                .option("sizeInMB", "1")
                .load(tifPath)
                .write
                .format("gdal")
                .option("ext", "tif")
                .mode("append")
                .save(outDir)

            val tifs = Files.list(Paths.get(outDir)).toList.asScala
                .filter(p => p.toString.endsWith(".tif"))
                .toList
            assert(tifs.nonEmpty, "expected at least one .tif output")
            tifs.foreach { p =>
                val fname = p.getFileName.toString
                assert(fname.head.isDigit,
                    s"default filename '$fname' starts with '${fname.head}'" +
                    " — Hadoop hidden-file filter would silently skip it")
            }
        } finally {
            val p = Paths.get(outDir)
            if (Files.exists(p)) {
                Files.list(p).toList.asScala.foreach(Files.deleteIfExists)
                Files.deleteIfExists(p)
            }
        }
    }

    test("GDAL RowWriter default filenames stay distinct when tiles share a cellid") {
        // Retile-family tiles carry their parent's cellid, so many tiles of one parent share it.
        // The default name must not be derived from the cellid alone, or the later tiles would
        // overwrite the first. Every written tile must land in its own file.
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString
        val outDir = Files.createTempDirectory("gdal_shared_cellid_out_").toString

        try {
            val shared = spark.read
                .format("gdal")
                .option("sizeInMB", "1")
                .load(tifPath)
                .withColumn("tile", col("tile").withField("cellid", lit(7L)))
            val rows = shared.count()
            rows should be > 1L

            shared.write
                .format("gdal")
                .option("ext", "tif")
                .mode("append")
                .save(outDir)

            val tifs = Files.list(Paths.get(outDir)).toList.asScala
                .filter(p => p.toString.endsWith(".tif"))
                .toList
            assert(tifs.size.toLong == rows, s"expected one file per tile ($rows), got ${tifs.size}")
        } finally {
            val p = Paths.get(outDir)
            if (Files.exists(p)) {
                Files.list(p).toList.asScala.foreach(Files.deleteIfExists)
                Files.deleteIfExists(p)
            }
        }
    }

    test("GDAL Data Source nameCol option controls output filename prefix") {
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString
        val outDir = Files.createTempDirectory("gdal_namecol_out_").toString

        try {
            // nameCol must reference a column that exists in the table's fixed
            // (source, tile) schema. Overwriting 'source' with a deterministic
            // string keeps arity correct and gives the writer a filename to use.
            val df = spark.read
                .format("gdal")
                .option("sizeInMB", "1")
                .load(tifPath)
                .withColumn("source", concat(lit("named_"), monotonically_increasing_id()))

            df.write
                .format("gdal")
                .option("nameCol", "source")
                .option("ext", "tif")
                .mode("append")
                .save(outDir)

            val tifs = Files.list(Paths.get(outDir)).toList.asScala
                .filter(p => p.toString.endsWith(".tif"))
                .toList
            assert(tifs.nonEmpty, "expected at least one .tif output")
            // every file should carry the nameCol prefix, not the default MurmurHash3 one
            tifs.foreach(p => assert(p.getFileName.toString.startsWith("named_"),
                s"expected nameCol prefix on ${p.getFileName}"))

            // validate at least one written file is a readable GDAL dataset
            val ds = gdal.Open(tifs.head.toString)
            assert(ds != null, s"GDAL could not open ${tifs.head}")
            ds.GetRasterBand(1).AsMDArray().GetStatistics().getValid_count should be >= 0L
        } finally {
            val p = Paths.get(outDir)
            if (Files.exists(p)) {
                Files.list(p).toList.asScala.foreach(Files.deleteIfExists)
                Files.deleteIfExists(p)
            }
        }
    }

}
