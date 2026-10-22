package com.databricks.labs.gbx.rasterx.ds

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.ds.gdal.GDAL_RowWriter
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.util.V2Tile
import com.databricks.labs.gbx.util.NodeFileCacheProbe.{entries, garbage, withNetCDFCopy, withScratchDir}
import com.databricks.labs.gbx.util.{HadoopUtils, NodeCacheRetentionOff, NodeFileManager, NodeFilePathUtil, SerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.ListHasAsScala
import org.apache.spark.sql.functions.{concat, lit, monotonically_increasing_id}

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

    test("GDAL writer replaces a directory-format (Zarr) output written again under the same name") {
        GDALManager.init(ExpressionConfig(spark))
        assume(gdal.GetDriverByName("Zarr") != null, "this GDAL build has no Zarr driver")
        val band = (b: String) =>
            Paths.get(getClass.getResource(s"/modis/MCD43A4.A2018185.h10v07.006.2018194033728_$b.TIF").toURI)
        // A (source, tile) row of one band's GeoTIFF, to be written as an uncompressed Zarr named "same"
        // (Zarr has no DEFLATE codec, the writer's default).
        val row = (b: String) => {
            val meta = Map("driver" -> "GTiff", "format" -> "Zarr", "compression" -> "NONE")
            val tile = V2Tile.row(cellid = 0L, raster = Files.readAllBytes(band(b)), metadata = SerializationUtil.toMapData(meta))
            InternalRow(UTF8String.fromString("same"), tile)
        }
        val checksum = (p: String) => {
            val ds = gdal.Open(p)
            try ds.GetRasterBand(1).Checksum()
            finally ds.delete()
        }
        val schema = StructType(Seq(StructField("source", StringType), StructField("tile", V2Tile.schema)))
        withScratchDir("gdal_zarr_out_") { out =>
            val writer = new GDAL_RowWriter(schema, out.toString, Some("source"), "zarr", 0, 0L, ExpressionConfig(spark))
            writer.write(row("B01"))
            val zarr = out.resolve("same.zarr")
            withClue(s"$zarr after the first write: ") { Files.isDirectory(zarr) shouldBe true }
            val stale = zarr.resolve("stale.marker") // an entry the second write does not make
            Files.write(stale, garbage)
            writer.write(row("B02"))
            withClue(s"$stale after the second write: ") { Files.exists(stale) shouldBe false }
            withClue(s"band 1 of $zarr after the second write: ") {
                checksum(zarr.toString) shouldBe checksum(band("B02").toString)
            }
        }
    }

    test("GDAL writer's local cleanup deletes a staging dir holding a non-empty directory output") {
        withScratchDir("gdal_writer_local_") { root =>
            val dir = root.resolve("stage")
            Files.createDirectories(dir.resolve("out.zarr").resolve("b"))
            Files.write(dir.resolve("out.zarr").resolve(".zarray"), garbage)
            Files.write(dir.resolve("out.zarr").resolve("b").resolve("0.0"), garbage)
            GDAL_RowWriter.deleteLocal(dir)
            withClue(s"$dir after the cleanup: ") { Files.exists(dir) shouldBe false }
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
