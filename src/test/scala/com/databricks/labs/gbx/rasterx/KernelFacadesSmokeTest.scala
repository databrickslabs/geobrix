package com.databricks.labs.gbx.rasterx

import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.util.concurrent.TimeUnit
import java.util.jar.JarOutputStream
import java.util.zip.ZipEntry

import com.databricks.labs.gbx.kernels.{BuildInfo, KernelGdal, L1Installer}
import com.databricks.labs.gbx.kernels.df.BngTessellateJob
import com.databricks.labs.gbx.kernels.uc.BngUc
import com.databricks.labs.gbx.kernels.udf.{BngEastNorthAsBngConnectUdf, BngEastNorthAsBngUdf, BuildInfoConnectUdf, GdalProbeConnectUdf, RstTransformCrsBytesUdf}
import com.databricks.labs.gbx.rasterx.expressions.agg.mvp.BngRasterizeCellsBytesUdf
import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver, TileBytes, TileBytesRefused}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.gdalconst.gdalconstConstants
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

    test("TileBytes.checkDecoded gives the decoded-size refusal; the ungated read opens a tile the gate refuses") {
        val conf = Some(ExpressionConfig(Map(TileBytes.ReadCapKey -> "1024"), null))
        assert(TileBytes.checkDecoded(1024L, conf).isEmpty)
        val refusal = TileBytes.checkDecoded(1025L, conf).get
        assert(refusal.getMessage.contains("over this compute's read cap (1 KiB)") &&
            refusal.getMessage.contains("Fix: retile it first (gbx_rst_retile)"))
        val tile = tinyGTiff() // 16 x 16 float32 = 1 KiB decoded
        TileBytes.useReadCap(Some(512L))
        try {
            intercept[TileBytesRefused](RasterDriver.readFromBytes(tile, Map("driver" -> "GTiff")))
            val ds = RasterDriver.readFromBytesUngated(tile, Map("driver" -> "GTiff"))
            try assert(ds.getRasterXSize == 16) finally RasterDriver.releaseDataset(ds)
        } finally TileBytes.useReadCap(None)
    }

    test("the read cap holds for the task that set it: a later task on the same thread reads at the default") {
        val sc = spark.sparkContext
        val n = 64 // more tasks than the pool has threads, so every thread has set a cap before any reads
        val set = sc.parallelize(1 to n, n).map { _ => TileBytes.useReadCap(Some(512L)); TileBytes.readCap }.collect()
        assert(set.forall(_ == 512L))
        val later = sc.parallelize(1 to n, n).map(_ => TileBytes.readCap).collect()
        assert(later.forall(_ == com.databricks.labs.gbx.kernels.spec.GdalSpecEngine.DecodedBudget), later.distinct.toSeq)
    }

    test("a zipped tile reports its driver from the path its read uses") {
        val tile = tinyGTiff()
        val options = Map("driver" -> "GTiff", "isZip" -> "true")
        val read = RasterDriver.readFromBytesUngated(tile, options)
        val readable = read != null
        if (readable) RasterDriver.releaseDataset(read)
        assert(RasterDriver.sourceDriverOf(tile, options).isDefined == readable)
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
        // the cell handlers answer on the slim classpath too (JTS and the grid, no Catalyst): area, ring size, EWKB sizes
        assert(lines.contains("UC cells: 1.0,9,97,25"), output)
        for (bad <- Seq("L2 bad: ", "UC bad: ")) {
            assert(lines.exists(l => l.startsWith(bad + "RAISES IllegalStateException") && l.contains("found 1001")), output)
        }
        // The raster facades run end to end on that classpath, natives included: no Catalyst or unsafe class on their path.
        // The larger duplicate wins and the empty pixel is NoData; a NaN beside 7.0 does not win, a cell of NaN burns NaN.
        assert(lines.contains("L2 rasterize: 7.0,NaN;3.0,2.0"), output)
        // the classpath really is slim: nothing of Catalyst, the unsafe types or Kryo can load in the child
        assert(lines.contains("loadable: none"), output)
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

    test("BngUc cell handlers match the session functions and give NULL for data errors") {
        assert(BngUc.cellArea("TQ3080") == 1.0 && BngUc.cellArea("TQ") == 10000.0 && BngUc.cellArea("TQ38SW") == 25.0)
        assert(BngUc.cellArea("XX9999") == null && BngUc.cellArea(null) == null)
        assert(BngUc.kRing("TQ3080", 0) == Seq("TQ3080"))
        val ring = BngUc.kRing("TQ3080", 1)
        assert(ring.size == 9 && ring.toSet.contains("TQ2979") && ring.toSet.contains("TQ3181"))
        assert(BngUc.kRing("XX9999", 1) == null && BngUc.kRing(null, 1) == null && BngUc.kRing("TQ3080", null) == null)
        // EWKB: byte order, type flagged with the SRID bit (0x20000000), then the SRID 27700
        def srid(b: Array[Byte]): Int = {
            val bb = java.nio.ByteBuffer.wrap(b).order(if (b(0) == 1) java.nio.ByteOrder.LITTLE_ENDIAN else java.nio.ByteOrder.BIG_ENDIAN)
            assert((bb.getInt(1) & 0x20000000) != 0, "EWKB carries an SRID")
            bb.getInt(5)
        }
        val cell = BngUc.asWkb("TQ3080")
        assert(srid(cell) == 27700)
        val poly = new org.locationtech.jts.io.WKBReader().read(cell)
        assert(poly.getEnvelopeInternal.toString == "Env[530000.0 : 531000.0, 180000.0 : 181000.0]")
        val centre = BngUc.centroid("TQ3080")
        assert(srid(centre) == 27700)
        val pt = new org.locationtech.jts.io.WKBReader().read(centre)
        assert(pt.getCoordinate.x == 530500.0 && pt.getCoordinate.y == 180500.0)
        assert(BngUc.asWkb("XX9999") == null && BngUc.centroid(null) == null)
    }

    test("BngUc cell handlers write the bytes the UC Python handlers write") {
        // the same expected hex as test_family_cross_tier.py: little-endian EWKB, SRID 27700, ring from the south-west corner
        def hex(b: Array[Byte]): String = b.map(x => f"${x & 0xff}%02x").mkString
        assert(hex(BngUc.asWkb("TQ3080")) ==
            "0103000020346c0000010000000500000000000000a02c20410000000000f9054100000000703420410000000000f9054100" +
            "00000070342041000000004018064100000000a02c2041000000004018064100000000a02c20410000000000f90541")
        assert(hex(BngUc.centroid("TQ3080")) == "0101000020346c0000000000008830204100000000a0080641")
    }

    test("BngTessellateJob tessellates a polygon column in one mapPartitions") {
        // 2800 x 800 m, offset 100 m into the cells it covers: three 1 km border cells
        val poly = "POLYGON((530100 180100, 532900 180100, 532900 180900, 530100 180900, 530100 180100))"
        val polys = spark.createDataFrame(Seq(Tuple1(poly))).toDF("wkt")
        val rows = BngTessellateJob.transform(spark, polys, 3).collect()
        assert(rows.map(_.getString(0)).toSet == Set("TQ3080", "TQ3180", "TQ3280"))
        assert(rows.forall(r => !r.getBoolean(1)) && math.abs(rows.map(_.getDouble(2)).sum - 2800.0 * 800.0) < 1e-6)
    }
}
