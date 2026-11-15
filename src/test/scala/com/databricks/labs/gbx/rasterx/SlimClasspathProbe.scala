package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.kernels.KernelGdal
import com.databricks.labs.gbx.kernels.uc.BngUc
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}


/** Run by [[KernelFacadesSmokeTest]] in a child JVM whose classpath is the facades, scala-library, JTS, the GDAL
  * bindings, spark-sql-api with what its `Row` needs, and what the GDAL start-up needs of Spark core, Hadoop and log4j
  * (what a standard-mode or Unity Catalog sandbox offers) but NOT Catalyst, the unsafe types or Kryo. It says which of
  * those could load (none should), and runs the raster facades end to end. */
object SlimClasspathProbe {

    private def attempt(body: => String): String =
        try body catch { case t: Throwable => "RAISES " + t.getClass.getSimpleName + ": " + t.getMessage }

    def main(args: Array[String]): Unit =
        if (args.headOption.contains("start-up")) startUp() else facades()

    /** The facades' GDAL start-up (KernelGdal: no session, an empty configuration), then the JNI it loaded. */
    private def startUp(): Unit = {
        // scalastyle:off println
        println("no-config start-up: " + attempt {
            KernelGdal.ensureInit()
            "loaded " + GDALManager.ensureJniLoaded(None).jni
        })
        // the driver policy decided without a cluster conf: on (fail-closed), so GDAL_SKIP names the refused drivers
        println("GDAL_SKIP: " + attempt(String.valueOf(org.gdal.gdal.gdal.GetConfigOption("GDAL_SKIP"))))
        // scalastyle:on println
    }

    private def facades(): Unit = {
        val absent = Seq("org.apache.spark.sql.catalyst.InternalRow", "org.apache.spark.unsafe.types.UTF8String", "com.esotericsoftware.kryo.Kryo")
        // scalastyle:off classforname
        val loadable = absent.filter(n => try { Class.forName(n); true } catch { case _: Throwable => false })
        // scalastyle:on classforname
        val lines = Seq(
          "UC good: " + attempt(BngUc.eastNorthAsBng(530000.0, 180000.0, "-3")),
          "UC bad: " + attempt(BngUc.eastNorthAsBng(530000.0, 180000.0, "1001")),
          "UC cells: " + attempt(Seq(BngUc.cellArea("TQ3080"), BngUc.kRing("TQ3080", 1).size, BngUc.asWkb("TQ3080").length,
            BngUc.centroid("TQ3080").length).mkString(",")),
          "loadable: " + (if (loadable.isEmpty) "none" else loadable.mkString(",")))
        // scalastyle:off println
        lines.foreach(println)
        // scalastyle:on println
    }
}
