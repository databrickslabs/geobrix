package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.expressions.accessors.RST_SRID
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.{CoordinateTransformation, SpatialReference}

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.{Callable, Executors, TimeUnit}
import scala.jdk.CollectionConverters._

/** The child JVM of [[GDALManagerNodeCopyTest]]: initialises GDAL from the native directory `args(0)` with the copy root
  * `args(1)`, writes an EPSG:27700 GeoTIFF to `/vsimem`, removes the wheel tree `args(2)`, then reads the GeoTIFF's
  * srid and transforms a point on new threads. Prints `JNI=`, `MARKER=`, `WHEEL_MAPPED=` (mapped files under the wheel),
  * `PRE=`, `GONE=`, `SRIDS=` (the distinct answers), `TRANSFORM=`, `PROJ_DATA=` and `DONE`.
  */
object NodeCopyNativeProbe {

    private def delete(f: File): Unit = {
        Option(f.listFiles()).foreach(_.foreach(delete))
        f.delete()
        ()
    }

    private def srid(path: String): String = {
        val ds = gdal.Open(path, gdalconstConstants.GA_ReadOnly)
        if (ds == null) s"ERROR open: ${gdal.GetLastErrorMsg}"
        else try RST_SRID.execute(ds).toString finally ds.delete()
    }

    private def transform(): String = {
        val (from, to) = (new SpatialReference(), new SpatialReference())
        val (a, b) = (from.ImportFromEPSG(4326), to.ImportFromEPSG(27700))
        if (a != 0 || b != 0) s"ERROR ImportFromEPSG returned $a and $b: ${gdal.GetLastErrorMsg}"
        else {
            from.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
            to.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
            val ct = CoordinateTransformation.CreateCoordinateTransformation(from, to)
            if (ct == null) s"ERROR no transformation: ${gdal.GetLastErrorMsg}"
            else {
                val xy = ct.TransformPoint(-0.1276, 51.5072)
                if (xy(0).isNaN || xy(0).isInfinite) "ERROR non-finite result" else s"ok ${xy(0).round},${xy(1).round}"
            }
        }
    }

    def main(args: Array[String]): Unit = {
        val Array(dir, copies, wheel) = args
        val conf = ExpressionConfig(
          Map(GDALManager.NativeDirKey -> dir, GDALManager.NativeCopyDirKey -> copies),
          new SerializableConfiguration(new Configuration())
        )
        try GDALManager.init(conf)
        catch {
            case e: Throwable =>
                println(s"INIT=ERROR ${e.getClass.getName}: ${e.getMessage}")
                System.exit(3)
        }
        val jni = GDALManager.ensureJniLoaded(Some(dir)).jni
        println(s"JNI=$jni")
        println(s"MARKER=${Files.isRegularFile(Paths.get(jni).getParent.getParent.getParent.resolve(NativeCopy.DoneMarker))}")
        val maps = Files.readAllLines(Paths.get("/proc/self/maps")).asScala
        println(s"WHEEL_MAPPED=${maps.count(_.contains(wheel))}")
        // an EPSG:27700 GeoTIFF: reading its srid back needs proj.db to identify the code
        val path = "/vsimem/node_copy_probe.tif"
        val ds = GDALManager.gtiffDriver().Create(path, 4, 4, 1, gdalconstConstants.GDT_Byte)
        val sr = new SpatialReference()
        sr.ImportFromEPSG(27700)
        ds.SetProjection(sr.ExportToWkt())
        ds.SetGeoTransform(Array(500000.0, 10.0, 0.0, 200000.0, 0.0, -10.0))
        ds.delete()
        println(s"PRE=${srid(path)}")
        delete(new File(wheel))
        println(s"GONE=${!Files.exists(Paths.get(wheel))}")
        // new threads: each has a new PROJ context, which opens proj.db at its first use
        val pool = Executors.newFixedThreadPool(4)
        val answers = (1 to 4).map(_ => pool.submit(new Callable[Seq[String]] {
            override def call(): Seq[String] = (1 to 25).map(_ => try srid(path) catch { case e: Throwable => s"ERROR $e" })
        }))
        val t = pool.submit(new Callable[String] {
            override def call(): String = try transform() catch { case e: Throwable => s"ERROR $e" }
        })
        println(s"SRIDS=${answers.flatMap(_.get(120, TimeUnit.SECONDS)).distinct.sorted.mkString(",")}")
        println(s"TRANSFORM=${t.get(120, TimeUnit.SECONDS)}")
        pool.shutdown()
        println(s"PROJ_DATA=${Option(gdal.GetConfigOption("PROJ_DATA")).getOrElse("<unset>")}")
        println("DONE")
    }
}
