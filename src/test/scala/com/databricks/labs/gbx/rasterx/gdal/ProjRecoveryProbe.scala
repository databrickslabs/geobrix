package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.operations.ProjDatabase
import com.databricks.labs.gbx.rasterx.expressions.accessors.RST_SRID
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.{SpatialReference, osr}

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.{Callable, Executors, TimeUnit}
import scala.jdk.CollectionConverters._

/** A child JVM of [[GDALManagerStaleNativeDirTest]]: loads GDAL in place from the native directory `args(0)` with the
  * user's `spark.gdal.PROJ_DATA` naming `args(2)` before the payload's PROJ data, names the byte-identical payload
  * `args(1)`, then removes the first payload's `share/proj` and reads an EPSG:27700 GeoTIFF's srid on a new thread
  * through [[ProjDatabase.guarded]], as `gbx_rst_srid` does. Prints `PRE=`, `GONE=`, `SRID=`, `PROJ_DATA=`, `PATH=`
  * (PROJ's search path), `LOADED=` (the loaded choice's PROJ data) and `DONE`.
  */
object ProjRecoveryProbe {

    private def srid(path: String): Int = {
        val ds = gdal.Open(path, gdalconstConstants.GA_ReadOnly)
        try RST_SRID.execute(ds) finally ds.delete()
    }

    private def onNewThread(body: => String): String = {
        val pool = Executors.newSingleThreadExecutor()
        try pool.submit(new Callable[String] {
            override def call(): String = try body catch { case e: Throwable => s"ERROR ${e.getClass.getName}: ${e.getMessage}" }
        }).get(120, TimeUnit.SECONDS)
        finally pool.shutdown()
    }

    def main(args: Array[String]): Unit = {
        val Array(first, second, user) = args
        val firstProj = s"$first/../share/proj"
        GDALManager.init(ExpressionConfig(
          Map(
            GDALManager.NativeDirKey -> first,
            GDALManager.NativeCopyDirKey -> "off",
            "spark.gdal.PROJ_DATA" -> s"$user:$firstProj"
          ),
          new SerializableConfiguration(new Configuration())
        ))
        GDALManager.ensureJniLoaded(Some(second)) // the next run's copy, while the first is still there
        val path = "/vsimem/proj_recovery_probe.tif"
        val ds = GDALManager.gtiffDriver().Create(path, 4, 4, 1, gdalconstConstants.GDT_Byte)
        val sr = new SpatialReference()
        sr.ImportFromEPSG(27700)
        ds.SetProjection(sr.ExportToWkt())
        ds.SetGeoTransform(Array(500000.0, 10.0, 0.0, 200000.0, 0.0, -10.0))
        ds.delete()
        println(s"PRE=${onNewThread(srid(path).toString)}")
        Option(new File(firstProj).listFiles()).foreach(_.foreach(_.delete()))
        println(s"GONE=${!Files.exists(Paths.get(firstProj, "proj.db"))}")
        println(s"SRID=${onNewThread(ProjDatabase.guarded(() => srid(path))(_ == 0).toString)}")
        println(s"PROJ_DATA=${Option(gdal.GetConfigOption("PROJ_DATA")).getOrElse("<unset>")}")
        println(s"PATH=${Option(osr.GetPROJSearchPaths()).map(_.asScala.mkString(":")).getOrElse("<unset>")}")
        println(s"LOADED=${GDALManager.ensureJniLoaded(None).projData.getOrElse("<none>")}")
        println("DONE")
    }
}
