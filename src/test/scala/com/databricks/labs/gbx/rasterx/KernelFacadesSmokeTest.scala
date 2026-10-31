package com.databricks.labs.gbx.rasterx

import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.util.concurrent.TimeUnit
import java.util.jar.JarOutputStream
import java.util.zip.ZipEntry

import com.databricks.labs.gbx.kernels.{BuildInfo, KernelGdal, L1Installer}
import com.databricks.labs.gbx.kernels.df.{BngTessellateJob, ViewshedTowersJvm}
import com.databricks.labs.gbx.kernels.uc.BngUc
import com.databricks.labs.gbx.kernels.udf.{BngEastNorthAsBngConnectUdf, BngEastNorthAsBngUdf, BngTessellateUdf, BuildInfoConnectUdf, GdalProbeConnectUdf, RetileKernel, RstMergeArrayBytesUdf, RstRetileArrayBytesUdf, RstRetileCountBytesUdf, RstRetilePartBytesUdf, RstTransformCrsBytesUdf}
import com.databricks.labs.gbx.rasterx.expressions.agg.mvp.BngRasterizeCellsBytesUdf
import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.expressions.mvp.RST_MvpMaterialize
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.util.V2Tile
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.gdalconst.gdalconstConstants
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.osr.SpatialReference

import scala.jdk.CollectionConverters._

/** One smoke per Catalyst-free facade: the JAR identity, the L1 installer, the Java UDFs and the UC Scala handlers. */
class KernelFacadesSmokeTest extends PlanTest with SilentSparkSession {

    private def tinyGTiff(fill: Float = 1.0f, west: Double = -1.0): Array[Byte] = {
        KernelGdal.ensureInit()
        val ds = GDALManager.memDriver().Create("", 16, 16, 1, gdalconstConstants.GDT_Float32)
        ds.SetGeoTransform(Array(west, 0.125, 0.0, 51.0, 0.0, -0.125))
        val sr = new SpatialReference()
        sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        ds.GetRasterBand(1).WriteRaster(0, 0, 16, 16, Array.fill(256)(fill))
        try RasterDriver.writeToBytes(ds, Map("driver" -> "GTiff")) finally RasterDriver.releaseDataset(ds)
    }

    private def vsimemEntries(): Set[String] =
        Option(org.gdal.gdal.gdal.ReadDir("/vsimem/")).map(_.toArray.map(_.toString).toSet).getOrElse(Set.empty)

    test("BuildInfo reads version and sha from the one JAR, and says so when it cannot") {
        assert(BuildInfo.identity().split('|').length == 2)
        val dir = Files.createTempDirectory("gbx-buildinfo")
        val jar = dir.resolve("geobrix-9.9.9.jar")
        val out = new JarOutputStream(Files.newOutputStream(jar))
        try {
            out.putNextEntry(new ZipEntry("META-INF/maven/com.databricks.labs/geobrix/pom.properties"))
            out.write("version=9.9.9\n".getBytes("UTF-8"))
            out.closeEntry()
        } finally out.close()
        val sha = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(jar)).map("%02x".format(_)).mkString
        assert(BuildInfo.identityAt(jar.toUri) == s"9.9.9|$sha")
        val bare = dir.resolve("geobrix-bare.jar") // a JAR that lost its pom.properties: unknown version, real sha
        val bareOut = new JarOutputStream(Files.newOutputStream(bare))
        try {
            bareOut.putNextEntry(new ZipEntry("README"))
            bareOut.write("x".getBytes("UTF-8"))
            bareOut.closeEntry()
        } finally bareOut.close()
        val bareSha = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(bare)).map("%02x".format(_)).mkString
        assert(BuildInfo.identityAt(bare.toUri) == s"unknown|$bareSha")
        Files.delete(bare)
        assert(BuildInfo.identityAt(dir.toUri) == "dev|dev") // a classes directory is a development run
        assert(BuildInfo.identityAt(null) == "unknown|unknown") // never a silent dev
        Files.delete(jar)
        assert(BuildInfo.identityAt(jar.toUri) == "unknown|unknown")
        Files.delete(dir)
    }

    test("L1Installer installs a companion under the manifest name and reports arities") {
        val fqn = "com.databricks.labs.gbx.gridx.bng.BNG_EastNorthAsBNG"
        assert(L1Installer.acceptedArities(fqn, 2, 3) == "3")
        assert(L1Installer.install(spark, "gbx_bng_eastnorthasbng", fqn) == "ok")
        assert(L1Installer.install(spark, "gbx_other", fqn) == "name-mismatch:gbx_bng_eastnorthasbng")
        assert(spark.sql("SELECT gbx_bng_eastnorthasbng(530000d, 180000d, 3)").head().getString(0) == "TQ3080")
        // The same contract as the other levels: digit strings and INT coordinates are accepted; a resolution BNG
        // does not define, or a non-number, raises and names the value.
        assert(spark.sql("SELECT gbx_bng_eastnorthasbng(530000, 180000, '-3')").head().getString(0) == "TQ38SW")
        for (bad <- Seq("1001", "0", "7", "'1001'", "'abc'")) {
            val e = intercept[Exception](spark.sql(s"SELECT gbx_bng_eastnorthasbng(530000d, 180000d, $bad)").head())
            assert(e.getMessage.contains(bad.stripPrefix("'").stripSuffix("'")), s"resolution $bad: ${e.getMessage}")
        }
        val easting = intercept[Exception](spark.sql("SELECT gbx_bng_eastnorthasbng('abc', 180000d, 3)").head())
        assert(easting.getMessage.contains("abc"), easting.getMessage)
    }

    test("BngEastNorthAsBngUdf coerces SQL values and returns NULL for data errors") {
        val f = new BngEastNorthAsBngUdf()
        assert(f.call(530000.0, 180000.0, "1km") == "TQ3080")
        assert(f.call(530000.0, 180000.0, "3") == "TQ3080" && f.call(530000.0, 180000.0, "-3") == "TQ38SW") // digits
        assert(f.call(java.math.BigDecimal.valueOf(530000L), 180000.0, Integer.valueOf(3)) == "TQ3080")
        assert(f.call(null, 180000.0, 3) == null && f.call(Double.NaN, 180000.0, 3) == null)
        // PARAMETER errors raise and name the value; they never become a wrong cell or NULL.
        for (bad <- Seq[Any](1000, 0, 7, -7, java.lang.Long.valueOf(4294967299L), "3km")) {
            val e = intercept[Exception](f.call(530000.0, 180000.0, bad))
            assert(e.getMessage.contains(bad.toString), s"resolution $bad: ${e.getMessage}")
        }
        assert(intercept[Exception](f.call("abc", 180000.0, 3)).getMessage.contains("abc"))
    }

    test("the Java shells for standard access mode give the Scala answers, with the typed parameters SQL binds") {
        val shell = new BngEastNorthAsBngConnectUdf() // (double, double, string): an INT resolution arrives as digits
        val scala = new BngEastNorthAsBngUdf()
        for (res <- Seq("3", "-3", "1km")) {
            assert(shell.call(530000.0, 180000.0, res) == scala.call(530000.0, 180000.0, res), res)
        }
        assert(shell.call(530000.0, 180000.0, "3") == "TQ3080")
        assert(shell.call(null, 180000.0, "3") == null && shell.call(Double.NaN, 180000.0, "3") == null)
        assert(intercept[Exception](shell.call(530000.0, 180000.0, "3km")).getMessage.contains("3km"))
        assert(new BuildInfoConnectUdf().call() == BuildInfo.identity())
        assert(new GdalProbeConnectUdf().call() == KernelGdal.probe())
    }

    test("RstTransformCrsBytesUdf warps bytes and releases its datasets") {
        val input = tinyGTiff()
        val before = vsimemEntries()
        val out = new RstTransformCrsBytesUdf().call(input, "EPSG:3857")
        assert(new RstTransformCrsBytesUdf().call(Array[Byte](1, 2, 3), "EPSG:3857") == null)
        assert(new RstTransformCrsBytesUdf().call(input, "EPSG:not-a-crs") == null)
        assert(vsimemEntries() == before, "the facade left in-memory GDAL files behind")
        val ds = RasterDriver.readFromBytes(out, Map("driver" -> "GTiff"))
        try assert(ds.GetProjection().contains("Pseudo-Mercator")) finally RasterDriver.releaseDataset(ds)
    }

    // The first band of a GTiff as rows of pixels, and its size.
    private def decode(bytes: Array[Byte]): (Int, Int, Seq[Seq[Double]]) = {
        val ds = RasterDriver.readFromBytes(bytes, Map("driver" -> "GTiff"))
        try {
            val (w, h) = (ds.getRasterXSize, ds.getRasterYSize)
            val all = new Array[Double](w * h)
            ds.GetRasterBand(1).ReadRaster(0, 0, w, h, all)
            (w, h, all.toSeq.grouped(w).toSeq)
        } finally RasterDriver.releaseDataset(ds)
    }

    test("RstMergeArrayBytesUdf mosaics rasters in any order, drops one that does not open, and releases its datasets") {
        val (west, east) = (tinyGTiff(1.0f), tinyGTiff(2.0f, west = 1.0))
        val f = new RstMergeArrayBytesUdf()
        val before = vsimemEntries()
        val out = f.call(Seq(east, west))
        val (w, h, rows) = decode(out)
        assert((w, h) == (32, 16) && rows.head.take(16).forall(_ == 1.0f) && rows.head.drop(16).forall(_ == 2.0f))
        assert(java.util.Arrays.equals(out, f.call(Seq(west, east))), "the order of the input must not matter")
        assert(decode(f.call(Seq(west, Array[Byte](1, 2, 3))))._1 == 16, "a member that does not open is dropped")
        assert(f.call(Seq.empty) == null && f.call(null) == null && f.call(Seq(Array[Byte](1, 2, 3))) == null)
        assert(vsimemEntries() == before, "the facade left in-memory GDAL files behind")
    }

    test("BngRasterizeCellsBytesUdf burns a canvas: the largest value of a repeated cell, 1.0 for a NULL value") {
        val f = new BngRasterizeCellsBytesUdf()
        val cells = Seq(Row("TQ3080", 1.0), Row("TQ3080", 3.0), Row("TQ3180", 2.0), Row("TQ3081", null))
        def burn(c: Seq[Row]): Array[Byte] = f.call(c, 530000.0, 180000.0, 532000.0, 182000.0, 2, 2)
        val before = vsimemEntries()
        assert(decode(burn(cells))._3 == Seq(Seq(1.0, -9999.0), Seq(3.0, 2.0)))
        assert(decode(burn(cells.reverse))._3 == decode(burn(cells))._3, "the order of the rows must not matter")
        val nan = decode(burn(Row("TQ3181", Double.NaN) +: cells))._3
        assert(nan.head(1).isNaN && nan(1) == Seq(3.0, 2.0), "a NaN value burns NaN")
        assert(burn(Seq(Row(null, 1.0))) == null && burn(Seq.empty) == null, "no cell id, nothing to burn")
        // A NaN beside numbers in one cell never wins, whatever the order or the number of rows (an invalid sort order
        // can make the sort raise on 32 rows or more); a cell of nothing but NaN burns NaN.
        val crowd = Row("TQ3080", Double.NaN) +: (1 to 200).map(i => Row("TQ3080", i.toDouble)) :+ Row("TQ3180", Double.NaN)
        for (rows <- Seq(crowd, crowd.reverse, scala.util.Random.shuffle(crowd))) {
            val px = decode(burn(rows))._3
            assert(px.head.forall(_ == -9999.0) && px(1)(0) == 200.0 && px(1)(1).isNaN, px.toString)
        }
        assert(vsimemEntries() == before, "the facade left in-memory GDAL files behind")
    }

    /** [[SlimClasspathProbe]] with `args` in a child JVM whose classpath is the facades, scala-library, JTS, the GDAL bindings,
      * spark-sql-api with what its `Row` needs, and what the GDAL start-up needs of Spark core, Hadoop and log4j: no
      * Catalyst, no unsafe types, no Kryo, and no spark-network-common. Its output, once it exited 0. */
    private def runSlim(args: String*): String = {
        def where(c: Class[_]): String = Paths.get(c.getProtectionDomain.getCodeSource.getLocation.toURI).toString
        // scalastyle:off classforname
        def whereNamed(name: String): String = where(Class.forName(name)) // a class that is private[spark]
        // scalastyle:on classforname
        val classpath = Seq(
          where(classOf[BngEastNorthAsBngUdf]), // target/classes
          where(SlimClasspathProbe.getClass), // target/test-classes: the probe only is used
          where(classOf[scala.collection.immutable.List[_]]), // scala-library
          where(classOf[org.locationtech.jts.geom.Geometry]), // jts-core
          where(classOf[org.apache.spark.sql.api.java.UDF3[_, _, _, _]]), // spark-sql-api
          // What `Row` (the element of the tessellate facade's array) loads from the API side: its error classes
          // and JSON support. Still no Catalyst, no unsafe types and no Kryo, which the probe checks are absent.
          whereNamed("org.apache.spark.SparkUnsupportedOperationException"), // spark-common-utils
          where(classOf[org.json4s.JValue]), // json4s-ast
          where(classOf[org.json4s.Formats]), // json4s-core
          where(classOf[org.json4s.jackson.JsonMethods.type]), // json4s-jackson
          where(classOf[com.fasterxml.jackson.core.JsonParser]), // jackson-core
          where(classOf[com.fasterxml.jackson.databind.ObjectMapper]), // jackson-databind
          where(classOf[com.fasterxml.jackson.annotation.JsonProperty]), // jackson-annotations
          where(classOf[org.slf4j.Logger]), // slf4j-api
          where(classOf[org.gdal.gdal.Dataset]), // the GDAL bindings (the natives are the image's, loaded by the start-up)
          // What the GDAL start-up of a facade (`KernelGdal.ensureInit`) needs of the cluster: a Hadoop configuration, the
          // Spark class that wraps one, and log4j. None of them brings Catalyst, the unsafe types or Kryo.
          where(classOf[org.apache.spark.util.SerializableConfiguration]), // spark-core
          where(classOf[org.apache.hadoop.conf.Configuration]), // hadoop-client-api
          whereNamed("org.apache.hadoop.shaded.com.ctc.wstx.io.InputBootstrapper"), // hadoop-client-runtime
          where(classOf[org.apache.logging.log4j.Logger]), // log4j-api
          where(classOf[org.apache.logging.log4j.core.Filter]) // log4j-core
        ).distinct.mkString(java.io.File.pathSeparator)
        val javaBin = Paths.get(System.getProperty("java.home"), "bin", "java").toString
        val child = new ProcessBuilder((Seq(javaBin, "-cp", classpath, SlimClasspathProbe.getClass.getName.stripSuffix("$")) ++ args): _*)
            .redirectErrorStream(true).start()
        val output = scala.io.Source.fromInputStream(child.getInputStream, "UTF-8").mkString
        assert(child.waitFor(60, TimeUnit.SECONDS) && child.exitValue() == 0, output)
        output
    }

    test("the facades' GDAL start-up on a slim classpath reads no Spark class it cannot load") {
        // KernelGdal's start-up (a Java UDF or UC handler: no session, an empty configuration). The driver policy's switch
        // falls back to the JVM's SparkConf (SparkEnv); Spark core is on this classpath but spark-network-common is not,
        // so loading SparkEnv fails (NoClassDefFoundError: ...SecretKeyHolder). That must mean "no cluster conf", not a
        // failed GDAL start-up.
        val lines = runSlim("start-up").linesIterator.toSeq
        assert(lines.exists(_.startsWith("no-config start-up: loaded ")), lines.mkString("\n"))
        // with no cluster conf the policy stays on (fail-closed): GDAL skips every refused driver
        val skipped = lines.find(_.startsWith("GDAL_SKIP: ")).map(_.stripPrefix("GDAL_SKIP: ").split(" ").toSet)
        assert(skipped.exists(s => GDALManager.DefaultSkippedDrivers.split(" ").forall(s.contains)), lines.mkString("\n"))
    }

    test("the BNG L2 and UC facades run on a slim classpath: no Catalyst, no unsafe, no Kryo") {
        val output = runSlim()
        val lines = output.linesIterator.toSeq
        assert(lines.contains("L2 good: TQ3080"), output)
        assert(lines.contains("UC good: TQ38SW"), output)
        for (bad <- Seq("L2 bad: ", "UC bad: ")) {
            assert(lines.exists(l => l.startsWith(bad + "RAISES IllegalStateException") && l.contains("found 1001")), output)
        }
        // the tessellate facade: six chips for the offset polygon, none of them a core cell, a bad resolution named
        assert(lines.contains("L2 tessellate good: 6 TQ3080"), output)
        assert(lines.exists(l => l.startsWith("L2 tessellate bad: RAISES IllegalStateException") && l.contains("found 3km")), output)
        // The raster facades run end to end on that classpath, natives included: no Catalyst or unsafe class on their path.
        // The larger duplicate wins and the empty pixel is NoData; a NaN beside 7.0 does not win, a cell of NaN burns NaN.
        assert(lines.contains("L2 rasterize: 7.0,NaN;3.0,2.0"), output)
        assert(lines.contains("L2 merge: 4x2"), output)
        // the classpath really is slim: nothing of Catalyst, the unsafe types or Kryo can load in the child
        assert(lines.contains("loadable: none"), output)
    }

    test("the retile facades keep to the slim API surface (their classes name no Catalyst, unsafe or Kryo type)") {
        // They need GDAL natives, so they cannot run on the slim classpath; their constant pools are checked instead.
        val dir = Paths.get(classOf[RstRetileCountBytesUdf].getProtectionDomain.getCodeSource.getLocation.toURI)
        val names = Seq("RetileKernel", "RetileKernel$", "RstRetileCountBytesUdf", "RstRetilePartBytesUdf", "RstRetileArrayBytesUdf")
        val banned = Seq("org/apache/spark/sql/catalyst", "org/apache/spark/unsafe", "com/esotericsoftware")
        for (n <- names; b <- banned) {
            val bytes = Files.readAllBytes(dir.resolve("com/databricks/labs/gbx/kernels/udf/" + n + ".class"))
            assert(!new String(bytes, "ISO-8859-1").contains(b), s"$n names $b")
        }
    }

    test("BngUc handler matches L1 and takes digit resolutions") {
        assert(BngUc.eastNorthAsBng(530000.0, 180000.0, "3") == "TQ3080")
        assert(BngUc.eastNorthAsBng(530000.0, 180000.0, "-3") == "TQ38SW")
        for (bad <- Seq("1000", "0", "99999999999", "3km")) {
            val e = intercept[Exception](BngUc.eastNorthAsBng(530000.0, 180000.0, bad))
            assert(e.getMessage.contains(bad), s"resolution $bad: ${e.getMessage}")
        }
        assert(BngUc.gdalVersion().startsWith("ok:"))
    }

    test("BngTessellateUdf returns the chips of gbx_bng_tessellate and raises for a bad resolution") {
        // 2800 x 1800 m, offset 100 m into the cells it covers: six 1 km border cells
        val offset = "POLYGON((530100 180100, 532900 180100, 532900 181900, 530100 181900, 530100 180100))"
        val f = new BngTessellateUdf()
        val chips = f.call(offset, 3, false).asScala.toSeq
        assert(chips.map(_.getString(0)).toSet == Set("TQ3080", "TQ3180", "TQ3280", "TQ3081", "TQ3181", "TQ3281"))
        assert(chips.forall(c => !c.getBoolean(1) && c.get(2) != null))
        assert(f.call(null, 3, false) == null && f.call(offset, null, false) == null)
        assert(f.call(offset.getBytes("UTF-8"), 3, false) == null) // bytes that are not a geometry: DATA, NULL
        assert(intercept[Exception](f.call(offset, "3km", false)).getMessage.contains("3km"))
    }

    test("BngTessellateJob tessellates a polygon column in one mapPartitions") {
        // 2800 x 800 m, offset 100 m into the cells it covers: three 1 km border cells
        val poly = "POLYGON((530100 180100, 532900 180100, 532900 180900, 530100 180900, 530100 180100))"
        val polys = spark.createDataFrame(Seq(Tuple1(poly))).toDF("wkt")
        val rows = BngTessellateJob.transform(spark, polys, 3).collect()
        assert(rows.map(_.getString(0)).toSet == Set("TQ3080", "TQ3180", "TQ3280"))
        assert(rows.forall(r => !r.getBoolean(1)) && math.abs(rows.map(_.getDouble(2)).sum - 2800.0 * 800.0) < 1e-6)
    }

    test("the retile facades cut windows from bytes") {
        val input = tinyGTiff()
        assert(new RstRetileCountBytesUdf().call(input, 16, 16) == 1)
        assert(new RstRetileCountBytesUdf().call(input, 8, 8) == 4)
        assert(new RstRetilePartBytesUdf().call(input, 16, 16, 0).nonEmpty)
        assert(new RstRetileArrayBytesUdf().call(input, 8, 8).size == 4)
        assert(new RstRetileCountBytesUdf().call(null, 8, 8) == null && new RstRetilePartBytesUdf().call(input, 8, 8, null) == null)
        // The error contract of the other levels, for bytes: raster bytes that do not open are one error tile, that is
        // one piece whose bytes are NULL (what the wrapper turns into an error tile); a parameter that is no number
        // raises, and so does an index the count never gave.
        val junk = Array.fill[Byte](64)(7)
        assert(new RstRetileCountBytesUdf().call(junk, 8, 8) == 1)
        assert(new RstRetilePartBytesUdf().call(junk, 8, 8, 0) == null)
        assert(new RstRetileArrayBytesUdf().call(junk, 8, 8).asScala.toSeq == Seq(null))
        assert(intercept[Exception](new RstRetileCountBytesUdf().call(input, "wide", 8)).getMessage.contains("wide"))
        intercept[IndexOutOfBoundsException](new RstRetilePartBytesUdf().call(input, 8, 8, 4))
        // a NULL window is data, as at L1 and L3: the same error tile, not a NullPointerException
        for (nulls <- Seq[(Any, Any)]((null, 8), (8, null))) {
            assert(new RstRetileCountBytesUdf().call(input, nulls._1, nulls._2) == 1)
            assert(new RstRetilePartBytesUdf().call(input, nulls._1, nulls._2, 0) == null)
            assert(new RstRetileArrayBytesUdf().call(input, nulls._1, nulls._2).asScala.toSeq == Seq(null))
        }
        // a window of 0 never advances: a parameter error that says what to do, on all three facades
        for (bad <- Seq((0, 8, "tile_width"), (8, -1, "tile_height"))) {
            for (call <- Seq[() => Any](
                   () => new RstRetileCountBytesUdf().call(input, bad._1, bad._2),
                   () => new RstRetilePartBytesUdf().call(input, bad._1, bad._2, 0),
                   () => new RstRetileArrayBytesUdf().call(input, bad._1, bad._2))) {
                val e = intercept[IllegalArgumentException](call())
                assert(e.getMessage.contains(s"${bad._3} must be 1 or more pixels") && e.getMessage.contains("Fix:"), e.getMessage)
            }
        }
        RetileKernel.clear()
    }

    test("RetileKernel tells two rasters that share a bucket apart by their bytes") {
        // 512 x 512 int32, uncompressed (1 MiB): above the size the key reads in full
        def plain(editRow: Int): Array[Byte] = {
            KernelGdal.ensureInit()
            val path = s"/vsimem/retile_pair_${java.util.UUID.randomUUID()}.tif"
            val ds = GDALManager.gtiffDriver().Create(path, 512, 512, 1, gdalconstConstants.GDT_Int32, Array("COMPRESS=NONE"))
            val pixels = Array.tabulate(512 * 512)(i => (i / 512 * 13 + i % 512 * 7) % 1000)
            if (editRow >= 0) pixels(editRow * 512 + 10) += 100000
            ds.GetRasterBand(1).WriteRaster(0, 0, 512, 512, pixels)
            ds.FlushCache()
            ds.delete()
            try org.gdal.gdal.gdal.GetMemFileBuffer(path) finally org.gdal.gdal.gdal.Unlink(path)
        }
        val a = plain(-1)
        // an edited copy whose few changed bytes fall between two sampled blocks: the same bucket, not the same raster
        val b = (100 until 500 by 7).iterator.map(plain).find(x => RetileKernel.key(x) == RetileKernel.key(a)).get
        assert(a.length == b.length && a.length > 524288 && !java.util.Arrays.equals(a, b))
        // the piece that holds the edit (not the last, so the raster stays open for the next call)
        def all(raster: Array[Byte]): Seq[Seq[Byte]] =
            new RstRetileArrayBytesUdf().call(raster, 256, 256).asScala.map(_.toSeq).toSeq
        val (pa, pb) = (all(a), all(b))
        val k = pa.indices.find(i => pa(i) != pb(i)).get
        assert(k < pa.length - 1, "the edit must be in a piece that is not the last")
        def piece(raster: Array[Byte]): Seq[Byte] = new RstRetilePartBytesUdf().call(raster, 256, 256, k).toSeq
        RetileKernel.clear()
        val ofA = piece(a)
        assert(RetileKernel.cached() == 1)
        val ofB = piece(b) // B right after A on one thread, as two rows of one task
        RetileKernel.clear()
        val ofBAlone = piece(b)
        assert(ofA != ofBAlone, "the edit must show in the piece")
        assert(ofB == ofBAlone, "B was cut from A's open raster")
        RetileKernel.clear()
    }

    test("RetileKernel keys a raster by a sample of its bytes, not all of them") {
        val big = Array.tabulate[Byte](1 << 20)(i => (i % 251).toByte)
        def with0(at: Int): Array[Byte] = { val c = big.clone(); c(at) = (c(at) + 1).toByte; c }
        assert(RetileKernel.key(big) == RetileKernel.key(big.clone()))
        // the first byte, a sampled block (65536 is the start of the first), and the length are in the key
        assert(RetileKernel.key(with0(0)) != RetileKernel.key(big) && RetileKernel.key(with0(65540)) != RetileKernel.key(big))
        assert(RetileKernel.key(big.take(big.length - 1)) != RetileKernel.key(big))
        // a byte between the sampled blocks is not: the key is a bucket, and `open` confirms a hit against the bytes
        assert(RetileKernel.key(with0(70000)) == RetileKernel.key(big))
        // a small raster is keyed by all of it
        val small = big.take(1000)
        assert(RetileKernel.key(small.updated(500, 1.toByte)) != RetileKernel.key(small.updated(500, 2.toByte)))
    }

    test("RetileKernel empties its cache when the Spark task that filled it ends") {
        val input = tinyGTiff()
        val before = vsimemEntries()
        val held = spark.sparkContext.parallelize(1 to 2, 2).map { _ => RetileKernel.open(input); RetileKernel.cached() }.collect()
        assert(held.toSeq == Seq(1, 1)) // each task did hold the raster while it ran
        assert(vsimemEntries() == before, "a finished task left a raster open")
    }

    test("RetileKernel keeps ONE raster open per thread, and drops it after the row's last piece") {
        RetileKernel.clear()
        val before = vsimemEntries()
        for (n <- 1 to 3) RetileKernel.open(tinyGTiff(n.toFloat))
        assert(RetileKernel.cached() == RetileKernel.Capacity && RetileKernel.Capacity == 1)
        // three opened, two pushed out and released: only the one still held keeps an in-memory file
        assert(vsimemEntries().size - before.size == 1, "a raster that was pushed out kept its in-memory file")
        RetileKernel.clear()
        assert(RetileKernel.cached() == 0 && vsimemEntries() == before, "clear left in-memory GDAL files behind")
        // the pieces of one row: the raster stays open until the last piece has been cut, then it goes
        val input = tinyGTiff()
        val part = new RstRetilePartBytesUdf()
        for (i <- 0 until 3) { assert(part.call(input, 8, 8, i).nonEmpty); assert(RetileKernel.cached() == 1, s"piece $i") }
        assert(part.call(input, 8, 8, 3).nonEmpty && RetileKernel.cached() == 0, "the last piece left the raster open")
        new RstRetileArrayBytesUdf().call(input, 8, 8) // one call is the whole row
        assert(RetileKernel.cached() == 0 && vsimemEntries() == before, "the array facade left the raster open")
    }

    test("ViewshedTowersJvm counts a tower's viewshed over two tiles, skips a tower in no tile, and gives back its holds") {
        // two 32 x 32 tiles of flat ground, 1 m pixels in EPSG:3857, side by side: x 0..32 and 32..64
        def flat(west: Double): Array[Byte] = {
            KernelGdal.ensureInit()
            val ds = GDALManager.memDriver().Create("", 32, 32, 1, gdalconstConstants.GDT_Float32)
            ds.SetGeoTransform(Array(west, 1.0, 0.0, 32.0, 0.0, -1.0))
            val sr = new SpatialReference()
            sr.ImportFromEPSG(3857)
            ds.SetProjection(sr.ExportToWkt())
            ds.GetRasterBand(1).WriteRaster(0, 0, 32, 32, Array.fill(32 * 32)(10.0f))
            try RasterDriver.writeToBytes(ds, Map("driver" -> "GTiff")) finally RasterDriver.releaseDataset(ds)
        }
        val dir = Files.createTempDirectory("gbx-towers")
        val tiles = Map("0,0" -> flat(0.0), "1,0" -> flat(32.0)).map { case (k, bytes) =>
            val p = dir.resolve(s"dsm_${k.replace(',', '_')}.tif")
            Files.write(p, bytes)
            k -> p.toString
        }
        val before = vsimemEntries()
        // "on_edge" stands on the seam of the two tiles; "far" is in the reach of no tile; "west" is outside the DSM with
        // a tile in reach, so it stands on no tile and has no row
        val towers = spark.createDataFrame(Seq(("on_edge", 32.0, 16.0, 5.0), ("far", 1000.0, 1000.0, 5.0), ("west", -10.0, 16.0, 5.0)))
            .toDF("tower_id", "observer_x", "observer_y", "observer_height")
        val rows = ViewshedTowersJvm.run(spark, towers, tiles.asJava, 0.0, 0.0, 32.0, 100.0, 1.6, 2).collect()
        try {
            assert(rows.map(_.getString(0)).toSeq == Seq("on_edge"), "a tower in no tile's reach has no row")
            val (visible, total) = (rows.head.getLong(3), rows.head.getLong(4))
            assert(total == 64 * 32 && visible == total, s"flat ground, a mast and a range past both tiles: $visible of $total")
            for (p <- tiles.values) assert(NodeFilePathUtil.readersOf(p) == 0, s"$p is still held")
            assert(vsimemEntries() == before, "the step left in-memory GDAL files behind")
            // A tile that cannot be read fails the query, naming it, and every hold comes back: GDAL's VRT would leave
            // the tile out and give a plausible count; a truncated one reads as zeros.
            val whole = Files.readAllBytes(Paths.get(tiles("1,0")))
            val broken = Map("garbage" -> "not a raster".getBytes("UTF-8"), "truncated" -> whole.take(whole.length / 2))
            for ((kind, bytes) <- broken) {
                val p = dir.resolve("dsm_2_0.tif")
                Files.write(p, bytes)
                try {
                    val atBroken = spark.createDataFrame(Seq(("t", 50.0, 16.0, 5.0))).toDF("tower_id", "observer_x", "observer_y", "observer_height")
                    val e = intercept[Exception](
                      ViewshedTowersJvm.run(spark, atBroken, (tiles + ("2,0" -> p.toString)).asJava, 0.0, 0.0, 32.0, 100.0, 1.6, 1).collect())
                    assert(e.getMessage.contains("dsm_2_0.tif"), s"$kind: ${e.getMessage}")
                    for (q <- tiles.values ++ Seq(p.toString)) assert(NodeFilePathUtil.readersOf(q) == 0, s"$kind: $q is still held")
                    assert(vsimemEntries() == before, s"$kind: the step left in-memory GDAL files behind")
                } finally Files.deleteIfExists(p)
            }
        } finally {
            tiles.values.foreach(p => Files.deleteIfExists(Paths.get(p)))
            Files.deleteIfExists(dir)
        }
    }

    test("gbx_mvp_rst_materialize decodes a virtual tile's window as ZSTD, refuses a remote path, and holds nothing") {
        val dir = Files.createTempDirectory("gbx-materialize")
        val file = dir.resolve("src.tif")
        Files.write(file, tinyGTiff())
        val conf = UTF8String.fromString(ExpressionConfig(spark).toB64)
        def virtual(path: String, window: Option[(Int, Int, Int, Int)]) = V2Tile.row(
          cellid = 7L, path = UTF8String.fromString(path),
          window = window.map { case (c, r, w, h) => new GenericInternalRow(Array[Any](c, r, w, h)) }.orNull)
        val before = vsimemEntries()
        // "file:" is not a local path, so the open goes through a node-file copy: the one hold that must come back
        val remote = s"file:${file.toString}"
        try {
            val out = RST_MvpMaterialize.eval(virtual(remote, Some((0, 0, 8, 8))), conf)
            assert(out.getLong(V2Tile.idx("cellid")) == 7L && V2Tile.getPath(out) == null, "cellid kept, path cleared")
            val win = V2Tile.getWindow(out)
            assert((win.getInt(0), win.getInt(1), win.getInt(2), win.getInt(3)) == (0, 0, 8, 8), "window kept")
            val ds = RasterDriver.readFromBytes(V2Tile.getRaster(out), Map("driver" -> "GTiff"))
            try {
                assert(ds.GetRasterXSize == 8 && ds.GetRasterYSize == 8, "an 8 x 8 tile")
                assert(ds.GetMetadata_Dict("IMAGE_STRUCTURE").get("COMPRESSION") == "ZSTD")
            } finally RasterDriver.releaseDataset(ds)
            assert(NodeFilePathUtil.readersOf(remote) == 0, "the node-file lease came back")
            // a tile with bytes already, and one that is not a raster
            assert(RST_MvpMaterialize.eval(out, conf) eq out, "a materialized tile is returned as it is")
            Files.write(dir.resolve("bad.tif"), "not a raster".getBytes("UTF-8"))
            val bad = RST_MvpMaterialize.eval(virtual(s"file:${dir.resolve("bad.tif")}", None), conf)
            assert(V2Tile.getRaster(bad) == null && V2Tile.getMetadata(bad) != null, "an error tile")
            assert(NodeFilePathUtil.readersOf(s"file:${dir.resolve("bad.tif")}") == 0, "the lease came back on a failed open")
            // a path from data may not name a remote location
            val refused = RST_MvpMaterialize.eval(virtual("/vsicurl/https://example.com/x.tif", None), conf)
            assert(V2Tile.getRaster(refused) == null, "a remote location is refused")
            // a plain local path decodes the whole file (no window): the tile keeps its cellid and gets bytes
            val whole = RST_MvpMaterialize.eval(virtual(file.toString, None), conf)
            assert(V2Tile.getRaster(whole) != null && V2Tile.getWindow(whole) == null && V2Tile.getPath(whole) == null, "a whole-file decode")
            assert(whole.getLong(V2Tile.idx("cellid")) == 7L)
            // the error tile says why: a scheme without "//" is a remote location too (the light gate reads it by urlsplit), a
            // clip or a pending instruction is not applied, a window must lie inside the raster (also when its sum overflows Int)
            def whyNot(out: org.apache.spark.sql.catalyst.InternalRow): String = {
                assert(V2Tile.getRaster(out) == null, "an error tile")
                val md = com.databricks.labs.gbx.util.SerializationUtil.createMap[String, String](V2Tile.getMetadata(out))
                md("last_error")
            }
            for (p <- Seq("hdfs:/n/x.tif", "https:example.com/x.tif", "HTTP:x/y.tif", "ftp://h/x.tif", "/vsizip//vsicurl/https://h/x.zip"))
                assert(RST_MvpMaterialize.remoteLocation(p).isDefined && whyNot(RST_MvpMaterialize.eval(virtual(p, None), conf)).contains("remote location"), p)
            // the same verdicts as `_gdal_policy.remote_location` where Java and Python differ in character handling: a
            // trailing "+" is an empty scheme part (refused), Python's IGNORECASE folds U+017F and the Kelvin sign into [a-z],
            // and its \s (the "/vsi" lookbehind) matches NBSP and U+001C
            for (p <- Seq("s3+://x", "\u017f3://x", "\u212aa://x", "\u00a0/vsicurl/https://h/x", "\u001c/vsicurl/https://h/x",
                          "X\u00a0/vsicurl/x"))
                assert(RST_MvpMaterialize.remoteLocation(p).isDefined, s"refused as the light gate refuses it: ${p.map(_.toInt.toHexString).mkString(" ")}")
            assert(RST_MvpMaterialize.remoteLocation("\u00a0/vsis3/b/k").isEmpty, "an allowed /vsi name after a NBSP")
            for (p <- Seq("s3://b/k.tif", "abfss://c@a.dfs.core.windows.net/k.tif", "dbfs:/Volumes/c/s/v/a.tif", "file:/tmp/a.tif",
                          "/Volumes/c/s/v/a.tif", "rel/a.tif", "C:\\x\\a.tif", "/vsizip//Volumes/c/s/v/a.zip"))
                assert(RST_MvpMaterialize.remoteLocation(p).isEmpty, p)
            val pending = V2Tile.row(cellid = 7L, path = UTF8String.fromString(file.toString),
              metadata = com.databricks.labs.gbx.util.SerializationUtil.toMapData[String, String](Map("pending_nodata" -> "0")))
            assert(whyNot(RST_MvpMaterialize.eval(pending, conf)).contains("pending instruction"))
            val clipped = V2Tile.row(cellid = 7L, path = UTF8String.fromString(file.toString), clipPolygon = Array[Byte](1, 2))
            assert(whyNot(RST_MvpMaterialize.eval(clipped, conf)).contains("clip"))
            for (w <- Seq((8, 8, 16, 16), (-1, 0, 4, 4), (0, 0, 0, 4), (2000000000, 0, 2000000000, 1), (0, 2000000000, 1, 2000000000)))
                assert(whyNot(RST_MvpMaterialize.eval(virtual(file.toString, Some(w)), conf)).contains("is not inside"), w.toString)
            assert(NodeFilePathUtil.readersOf(file.toString) == 0, "no hold left by the refusals")
            assert(vsimemEntries() == before, "in-memory GDAL files left behind")
        } finally {
            Files.deleteIfExists(file)
            Files.deleteIfExists(dir.resolve("bad.tif"))
            Files.deleteIfExists(dir)
        }
    }
}
