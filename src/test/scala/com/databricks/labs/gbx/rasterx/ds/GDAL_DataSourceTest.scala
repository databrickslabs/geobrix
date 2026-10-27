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
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, empty, include, noException}

import java.nio.ByteOrder
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import scala.jdk.CollectionConverters._
import scala.sys.process._
import scala.util.Try
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

    test("an invalid retention in the session conf fails a gdal read's planning and names the conf") {
        val modis = this.getClass.getResource("/modis/").toString
        withSessionRetention("soon") {
            val e = intercept[Exception](spark.read.format("gdal").load(modis).count())
            withClue(s"the error chain ${messageChain(e)}: ") {
                messageChain(e).exists(_.contains(NodeFileManager.RetentionKey)) shouldBe true
            }
        }
    }

    // GDAL_RowWriter driven directly with (source, tile) rows built from one band's GeoTIFF.
    private val writerSchema = StructType(Seq(StructField("source", StringType), StructField("tile", V2Tile.schema)))

    private def modisBand(b: String) =
        Paths.get(getClass.getResource(s"/modis/MCD43A4.A2018185.h10v07.006.2018194033728_$b.TIF").toURI)

    /** A (source, tile) row of one band's GeoTIFF; `name` is the source, used as the output name (nameCol). */
    private def bandRow(name: String, b: String, meta: Map[String, String] = Map("driver" -> "GTiff")): InternalRow = {
        val tile = V2Tile.row(cellid = 0L, raster = Files.readAllBytes(modisBand(b)), metadata = SerializationUtil.toMapData(meta))
        InternalRow(UTF8String.fromString(name), tile)
    }

    private def nameColWriter(root: java.nio.file.Path, ext: String): GDAL_RowWriter =
        new GDAL_RowWriter(writerSchema, root.toString, Some("source"), ext, 0, 0L, ExpressionConfig(spark))

    test("GDAL writer replaces a directory-format (Zarr) output written again under the same name") {
        GDALManager.init(ExpressionConfig(spark))
        assume(gdal.GetDriverByName("Zarr") != null, "this GDAL build has no Zarr driver")
        // Written as an uncompressed Zarr, so this test is about the replacement and not the codec.
        val asZarr = Map("driver" -> "GTiff", "format" -> "Zarr", "compression" -> "NONE")
        val checksum = (p: String) => {
            val ds = gdal.Open(p)
            try ds.GetRasterBand(1).Checksum()
            finally ds.delete()
        }
        withScratchDir("gdal_zarr_out_") { out =>
            val writer = nameColWriter(out, "zarr")
            writer.write(bandRow("same", "B01", asZarr))
            val zarr = out.resolve("same.zarr")
            withClue(s"$zarr after the first write: ") { Files.isDirectory(zarr) shouldBe true }
            val stale = zarr.resolve("stale.marker") // an entry the second write does not make
            Files.write(stale, garbage)
            writer.write(bandRow("same", "B02", asZarr))
            withClue(s"$stale after the second write: ") { Files.exists(stale) shouldBe false }
            withClue(s"band 1 of $zarr after the second write: ") {
                checksum(zarr.toString) shouldBe checksum(modisBand("B02").toString)
            }
        }
    }

    // Tile metadata that names the Zarr format and no codec: the writer's default codec (DEFLATE) applies.
    private val zarrDefaults = Map("driver" -> "GTiff", "format" -> "Zarr")

    private def requireZarrDriver(): Unit = {
        GDALManager.init(ExpressionConfig(spark))
        assume(gdal.GetDriverByName("Zarr") != null, "this GDAL build has no Zarr driver")
    }

    /** Band 1 of the raster at `path` as Int32 values, row by row. */
    private def bandValues(path: String): (Int, Int, Array[Int]) = {
        val ds = gdal.Open(path)
        try {
            val (w, h) = (ds.getRasterXSize, ds.getRasterYSize)
            val values = new Array[Int](w * h)
            ds.GetRasterBand(1).ReadRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Int32, values)
            (w, h, values)
        } finally ds.delete()
    }

    /** The `.zarray` of the data array of a Zarr store, as text: the array is named for the file. The store also holds
      * uncompressed arrays for its coordinates. */
    private def dataArrayText(store: java.nio.file.Path, name: String): String =
        Files.readString(store.resolve(name).resolve(".zarray"))

    private def md5LittleEndianInt32(values: Array[Int]): String = {
        val bytes = java.nio.ByteBuffer.allocate(values.length * 4).order(ByteOrder.LITTLE_ENDIAN)
        bytes.asIntBuffer().put(values)
        MessageDigest.getInstance("MD5").digest(bytes.array()).map(b => f"${b & 0xff}%02x").mkString
    }

    test("GDAL writer writes Zarr with default options and gdal reads the store back with the source's values") {
        requireZarrDriver()
        withScratchDir("gdal_zarr_default_") { out =>
            nameColWriter(out, "zarr").write(bandRow("dflt", "B01", zarrDefaults))
            val zarr = out.resolve("dflt.zarr")
            withClue(s"$zarr after the write: ") { Files.isDirectory(zarr) shouldBe true }
            val (w, h, written) = bandValues(zarr.toString)
            val (sw, sh, source) = bandValues(modisBand("B01").toString)
            (w, h) shouldBe ((sw, sh))
            withClue("pixel values read back from the Zarr store: ") { java.util.Arrays.equals(written, source) shouldBe true }
            // DEFLATE has no Zarr codec of that name: the store holds the same zlib stream as ZLIB.
            withClue(s"the data array of $zarr: ") { dataArrayText(zarr, "dflt") should include regex """"id"\s*:\s*"zlib"""" }
        }
    }

    test("GDAL writer writes Zarr with default options and zarr-python reads the store back with the source's values") {
        requireZarrDriver()
        val script =
            """import hashlib, sys, numpy as np, zarr
              |g = zarr.open_group(sys.argv[1], mode="r")
              |a = np.asarray(g[sys.argv[2]][...]) # the data array is named for the file; the store also holds coordinate arrays
              |print(a.shape[0], a.shape[1], hashlib.md5(np.ascontiguousarray(a.astype("<i4")).tobytes()).hexdigest())
              |""".stripMargin
        val python = Seq("python3", "-I", "-c", script)
        val probe = Try(Seq("python3", "-I", "-c", "import zarr, numpy").!(ProcessLogger(_ => ()))).getOrElse(-1)
        assume(probe == 0, "python3 with zarr and numpy is not available")
        withScratchDir("gdal_zarr_python_") { out =>
            nameColWriter(out, "zarr").write(bandRow("dflt", "B01", zarrDefaults))
            val zarr = out.resolve("dflt.zarr")
            val (stdout, stderr) = (new StringBuilder, new StringBuilder)
            val exit = (python ++ Seq(zarr.toString, "dflt")).!(ProcessLogger(l => stdout.append(l), l => stderr.append(l).append('\n')))
            val (sw, sh, source) = bandValues(modisBand("B01").toString)
            withClue(s"zarr-python exit $exit, stdout '$stdout', stderr: $stderr") {
                exit shouldBe 0
                stdout.toString.trim shouldBe s"$sh $sw ${md5LittleEndianInt32(source)}"
            }
        }
    }

    test("GDAL writer keeps a Zarr codec the tile names and refuses one the Zarr driver lacks") {
        requireZarrDriver()
        val (_, _, source) = bandValues(modisBand("B01").toString)
        withScratchDir("gdal_zarr_codec_") { out =>
            val writer = nameColWriter(out, "zarr")
            writer.write(bandRow("zstd", "B01", zarrDefaults + ("compression" -> "ZSTD")))
            val zstd = out.resolve("zstd.zarr")
            withClue(s"the data array of $zstd: ") { dataArrayText(zstd, "zstd") should include regex """"id"\s*:\s*"zstd"""" }
            withClue("pixel values read back from the ZSTD store: ") {
                java.util.Arrays.equals(bandValues(zstd.toString)._3, source) shouldBe true
            }
            // LZW is a TIFF codec: the error names the codec and the ones the Zarr driver does have.
            val e = intercept[IllegalArgumentException](writer.write(bandRow("lzw", "B01", zarrDefaults + ("compression" -> "LZW"))))
            e.getMessage should (include("LZW") and include("ZLIB") and include("ZSTD") and include("NONE"))
            withClue(s"output left for the refused write: ") { Files.exists(out.resolve("lzw.zarr")) shouldBe false }
        }
    }

    test("GDAL writer refuses an output name with a '..' segment, which would write outside its root") {
        GDALManager.init(ExpressionConfig(spark))
        withScratchDir("gdal_dotdot_out_") { out =>
            val root = Files.createDirectories(out.resolve("root"))
            val writer = nameColWriter(root, "tif")
            for (bad <- Seq("../escape", "a/../../escape")) {
                val e = intercept[IllegalArgumentException](writer.write(bandRow(bad, "B01")))
                withClue(s"the error for '$bad': ") { e.getMessage should (include(bad) and include("..")) }
            }
            val outside = {
                val st = Files.walk(out)
                try st.iterator().asScala.filter(Files.isRegularFile(_)).map(out.relativize(_).toString).toSet
                finally st.close()
            }
            withClue("files written under the scratch dir: ") { outside shouldBe empty }
        }
    }

    test("GDAL writer refuses an absolute output name, which would write outside its root") {
        GDALManager.init(ExpressionConfig(spark))
        withScratchDir("gdal_abs_out_") { out =>
            val root = Files.createDirectories(out.resolve("root"))
            val outside = out.resolve("outside") // an absolute path that is not under root
            val e = intercept[IllegalArgumentException](nameColWriter(root, "tif").write(bandRow(outside.toString, "B01")))
            e.getMessage should (include(outside.toString) and include("/"))
            withClue(s"the file an absolute name would have written at $outside.tif: ") {
                Files.exists(Paths.get(s"$outside.tif")) shouldBe false
            }
            withClue("files written under the scratch dir: ") {
                val st = Files.walk(out)
                try st.iterator().asScala.filter(Files.isRegularFile(_)).toList shouldBe empty
                finally st.close()
            }
        }
    }

    test("GDAL writer opens no tile for an output name that is not a valid local path") {
        GDALManager.init(ExpressionConfig(spark))
        // readFromBytes opens a tile from a /vsimem/temp_raster_* file; releasing the Dataset unlinks it.
        val tempRasters = () =>
            Option(gdal.ReadDir("/vsimem/")).map(_.asScala.map(_.toString).filter(_.startsWith("temp_raster_")).toSet)
                .getOrElse(Set.empty[String])
        withScratchDir("gdal_nul_out_") { out =>
            val writer = nameColWriter(out, "tif")
            val before = tempRasters()
            intercept[java.nio.file.InvalidPathException](writer.write(bandRow("bad\u0000name", "B01")))
            withClue("in-memory tiles left open by the failed write: ") { tempRasters().diff(before) shouldBe empty }
        }
    }

    // translateTo must release its two Datasets through one step that keeps the write's own error. A nested
    // try/finally would let the first release error replace the write's error and skip nothing visible.
    test("GDAL writer's translate step releases both Datasets and fails with its own error, not a release error") {
        requireZarrDriver()
        withScratchDir("gdal_release_out_") { out =>
            val opened = scala.collection.mutable.ArrayBuffer.empty[Boolean]
            val writer = new GDAL_RowWriter(writerSchema, out.toString, Some("source"), "zarr", 0, 0L, ExpressionConfig(spark)) {
                override protected[ds] def releaseDataset(ds: Dataset): Unit = {
                    opened += (ds != null)
                    try super.releaseDataset(ds)
                    finally throw new IllegalStateException(s"release ${opened.size} failed")
                }
            }
            // The translate fails after the tile is open: Zarr has no LZW codec.
            val e = intercept[IllegalArgumentException](
              writer.write(bandRow("rel", "B01", zarrDefaults + ("compression" -> "LZW")))
            )
            e.getMessage should include("LZW")
            withClue("Datasets handed to the release step (the opened tile, then the output that was never made): ") {
                opened.toList shouldBe List(true, false)
            }
            e.getSuppressed.map(_.getMessage).toList shouldBe List("release 1 failed", "release 2 failed")
        }
    }

    test("GDAL writer's release step keeps the write's own error and attaches a failed release to it") {
        val primary = new RuntimeException("translate failed")
        val released = scala.collection.mutable.ArrayBuffer.empty[String]
        val badRelease = () => { released += "first"; throw new IllegalStateException("release failed") }
        noException should be thrownBy GDAL_RowWriter.releaseAll(primary, badRelease, () => released += "second")
        released.toList shouldBe List("first", "second")
        primary.getSuppressed.map(_.getMessage).toList shouldBe List("release failed")
        // With no error of its own, the write fails with the first release error, the others attached to it.
        val e = intercept[IllegalStateException](
          GDAL_RowWriter.releaseAll(null, badRelease, () => throw new IllegalStateException("second failed"))
        )
        e.getMessage shouldBe "release failed"
        e.getSuppressed.map(_.getMessage).toList shouldBe List("second failed")
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
