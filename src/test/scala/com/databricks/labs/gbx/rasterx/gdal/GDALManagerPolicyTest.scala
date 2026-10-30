package com.databricks.labs.gbx.rasterx.gdal

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.io.Source

/** The driver policy on every GDAL init path, and its precedence over the `spark.gdal.*` conf.
  *
  * Each case runs [[FreshJvmPolicyProbe]] in a child JVM, so the probe's first GDAL use is the one the case names: this
  * JVM's GDAL is already initialized by the other suites. The child reads the native library the way the JVM does.
  */
class GDALManagerPolicyTest extends AnyFunSuite {

    private val Refused = GDALManager.DefaultSkippedDrivers.split(" ").toSet

    /** The probe's output as key -> value (`PRESENT` is the registered refused drivers, comma-separated). */
    private def probe(mode: String, extra: Seq[String] = Nil, env: Map[String, String] = Map.empty): Map[String, String] = {
        val java = new File(System.getProperty("java.home"), "bin/java").getPath
        val cmd = Seq(java, "-cp", System.getProperty("java.class.path"),
            "com.databricks.labs.gbx.rasterx.gdal.FreshJvmPolicyProbe", mode) ++ extra
        val pb = new ProcessBuilder(cmd: _*).redirectErrorStream(true)
        env.foreach { case (k, v) => pb.environment().put(k, v) }
        val proc = pb.start()
        val lines = mutable.ArrayBuffer.empty[String]
        val src = Source.fromInputStream(proc.getInputStream)
        try src.getLines().foreach(lines += _) finally src.close()
        proc.waitFor(300, TimeUnit.SECONDS)
        val out = lines.toSeq.filter(_.contains("=")).map { l =>
            val i = l.indexOf('=')
            l.take(i) -> l.drop(i + 1)
        }.toMap
        val done = lines.exists(_ == "DONE")
        assume(done, s"PRECONDITION MISSING: the probe JVM did not finish (is the GDAL JNI on this host?): ${lines.takeRight(3).mkString(" | ")}")
        out
    }

    test("a JVM whose first GDAL use is an OGR writer applies the policy") {
        val out = probe("ogr")
        assert(out("GDAL_SKIP") == GDALManager.DefaultSkippedDrivers, s"GDAL_SKIP was ${out("GDAL_SKIP")}")
        assert(out("GDAL_VRT_ENABLE_RAWRASTERBAND") == "NO")
        assert(out("PRESENT").split(",").filter(_.nonEmpty).toSet.intersect(Refused).isEmpty,
            s"refused drivers registered after an OGR-first init: ${out("PRESENT")}")
    }

    test("a forced OGR registration (OGR_SchemaInference) applies the policy") {
        val out = probe("ogr-force")
        assert(out("GDAL_SKIP") == GDALManager.DefaultSkippedDrivers, s"GDAL_SKIP was ${out("GDAL_SKIP")}")
        assert(out("PRESENT").split(",").filter(_.nonEmpty).toSet.intersect(Refused).isEmpty,
            s"refused drivers registered after a forced OGR registration: ${out("PRESENT")}")
    }

    test("a JVM whose first GDAL use is a raster init applies the policy") {
        val out = probe("raster")
        assert(out("GDAL_SKIP") == GDALManager.DefaultSkippedDrivers)
        assert(out("PRESENT").split(",").filter(_.nonEmpty).toSet.intersect(Refused).isEmpty)
    }

    test("spark.gdal.GDAL_SKIP cannot un-skip the refused drivers") {
        val out = probe("raster", Seq("spark.gdal.GDAL_SKIP=", "spark.gdal.GDAL_VRT_ENABLE_RAWRASTERBAND=YES"))
        assert(out("GDAL_SKIP") == GDALManager.DefaultSkippedDrivers, s"the conf overrode the policy: ${out("GDAL_SKIP")}")
        assert(out("GDAL_VRT_ENABLE_RAWRASTERBAND") == "NO", "the conf overrode the raw-band setting")
        assert(out("PRESENT").split(",").filter(_.nonEmpty).toSet.intersect(Refused).isEmpty)
    }

    test("a spark.gdal.GDAL_SKIP name is skipped as well as the refused drivers") {
        val out = probe("raster", Seq("spark.gdal.GDAL_SKIP=BMP"))
        assert(out("GDAL_SKIP").split(" ").toSet == Refused + "BMP", s"GDAL_SKIP was ${out("GDAL_SKIP")}")
        assert(out("PRESENT").split(",").filter(_.nonEmpty).toSet.intersect(Refused).isEmpty)
    }

    test("the session's conf switch turns the policy off at an init, and the cluster's at an OGR-first use") {
        for (out <- Seq(
              probe("raster", Seq(s"${GDALManager.PolicyConf}=off")),
              probe("ogr-cluster", Seq(s"${GDALManager.PolicyConf}=off"))
            )) {
            assert(out("GDAL_SKIP") == "<unset>", s"the conf switch left a skip list: ${out("GDAL_SKIP")}")
            assert(out("PRESENT").split(",").exists(_.nonEmpty), "with the switch off, the refused drivers register")
        }
    }

    test("the documented switch turns the skip off, on the OGR path too") {
        val out = probe("ogr", env = Map("GBX_GDAL_DRIVER_POLICY" -> "off"))
        assert(out("GDAL_SKIP") == "<unset>", s"the switch left a skip list: ${out("GDAL_SKIP")}")
        assert(out("PRESENT").split(",").filter(_.nonEmpty).toSet.nonEmpty,
            "with the switch off, GDAL registers its own drivers, the refused ones included")
    }
}
