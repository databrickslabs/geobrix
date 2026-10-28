package com.databricks.labs.gbx.rasterx

import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.util.concurrent.TimeUnit
import java.util.jar.JarOutputStream
import java.util.zip.ZipEntry

import com.databricks.labs.gbx.kernels.{BuildInfo, KernelGdal, L1Installer}
import com.databricks.labs.gbx.kernels.uc.BngUc
import com.databricks.labs.gbx.kernels.udf.{BngEastNorthAsBngUdf, BngTessellateUdf, RetileKernel, RstRetileArrayBytesUdf, RstRetileCountBytesUdf, RstRetilePartBytesUdf, RstTransformCrsBytesUdf}
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference

import scala.jdk.CollectionConverters._

/** One smoke per Catalyst-free facade: the JAR identity, the L1 installer, the Java UDFs and the UC Scala handlers. */
class KernelFacadesSmokeTest extends PlanTest with SilentSparkSession {

    private def tinyGTiff(fill: Float = 1.0f): Array[Byte] = {
        KernelGdal.ensureInit()
        val ds = GDALManager.memDriver().Create("", 16, 16, 1, gdalconstConstants.GDT_Float32)
        ds.SetGeoTransform(Array(-1.0, 0.125, 0.0, 51.0, 0.0, -0.125))
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

    test("the BNG L2 and UC facades run on a slim classpath: no Catalyst, no unsafe, no Kryo") {
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
          where(classOf[org.slf4j.Logger]) // slf4j-api
        ).distinct.mkString(java.io.File.pathSeparator)
        val javaBin = Paths.get(System.getProperty("java.home"), "bin", "java").toString
        val child = new ProcessBuilder(javaBin, "-cp", classpath, SlimClasspathProbe.getClass.getName.stripSuffix("$"))
            .redirectErrorStream(true).start()
        val output = scala.io.Source.fromInputStream(child.getInputStream, "UTF-8").mkString
        assert(child.waitFor(60, TimeUnit.SECONDS) && child.exitValue() == 0, output)
        val lines = output.linesIterator.toSeq
        assert(lines.contains("L2 good: TQ3080"), output)
        assert(lines.contains("UC good: TQ38SW"), output)
        for (bad <- Seq("L2 bad: ", "UC bad: ")) {
            assert(lines.exists(l => l.startsWith(bad + "RAISES IllegalStateException") && l.contains("found 1001")), output)
        }
        // the tessellate facade: six chips for the offset polygon, none of them a core cell, a bad resolution named
        assert(lines.contains("L2 tessellate good: 6 TQ3080"), output)
        assert(lines.exists(l => l.startsWith("L2 tessellate bad: RAISES IllegalStateException") && l.contains("found 3km")), output)
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
}
