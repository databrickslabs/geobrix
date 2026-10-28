package com.databricks.labs.gbx.rasterx

import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.util.concurrent.TimeUnit
import java.util.jar.JarOutputStream
import java.util.zip.ZipEntry

import com.databricks.labs.gbx.kernels.{BuildInfo, KernelGdal, L1Installer}
import com.databricks.labs.gbx.kernels.uc.BngUc
import com.databricks.labs.gbx.kernels.udf.{BngEastNorthAsBngUdf, RstTransformCrsBytesUdf}
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference

/** One smoke per Catalyst-free facade: the JAR identity, the L1 installer, the Java UDFs and the UC Scala handlers. */
class KernelFacadesSmokeTest extends PlanTest with SilentSparkSession {

    private def tinyGTiff(): Array[Byte] = {
        KernelGdal.ensureInit()
        val ds = GDALManager.memDriver().Create("", 16, 16, 1, gdalconstConstants.GDT_Float32)
        ds.SetGeoTransform(Array(-1.0, 0.125, 0.0, 51.0, 0.0, -0.125))
        val sr = new SpatialReference()
        sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        ds.GetRasterBand(1).WriteRaster(0, 0, 16, 16, Array.fill(256)(1.0f))
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
        val classpath = Seq(
          where(classOf[BngEastNorthAsBngUdf]), // target/classes
          where(SlimClasspathProbe.getClass), // target/test-classes: the probe only is used
          where(classOf[scala.collection.immutable.List[_]]), // scala-library
          where(classOf[org.locationtech.jts.geom.Geometry]), // jts-core
          where(classOf[org.apache.spark.sql.api.java.UDF3[_, _, _, _]]) // spark-sql-api
        ).mkString(java.io.File.pathSeparator)
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
}
