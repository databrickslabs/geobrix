package com.databricks.labs.gbx.rasterx.gdal

import java.io.File
import java.nio.file.{Files, Paths}
import org.gdal.gdal.gdal
import org.gdal.osr.{CoordinateTransformation, SpatialReference}

/** The child JVM of [[GDALManagerStaleNativeDirTest]]: loads the GDAL JNI of native directory `args(0)`, uses PROJ once,
  * deletes the tree `args(2)` (a notebook's Python environment, torn down when its run ends, with the first native
  * directory in it), then names the byte-identical payload `args(1)` the way the next run's expressions do and uses PROJ
  * on a new thread (a new PROJ context). Prints `PRE=ok`, `LATER=ok` or `LATER=ERROR <message>`, `PROJ_DATA=`,
  * `FIND_PRE=` (a GDAL data file looked up before it), `GDAL_DATA=`, `JNI=` (the loaded JNI path), `FIND=` (a GDAL data file looked up after the delete), `TRANSFORM=`
  * (EPSG:4326 to EPSG:27700 of one point) and `DONE`.
  */
object StaleNativeDirProbe {

    /** Removes the tree, links as links: a link to a directory is not entered (its target is the staged payload's). */
    private def delete(f: File): Unit = {
        if (!Files.isSymbolicLink(f.toPath)) Option(f.listFiles()).foreach(_.foreach(delete))
        f.delete()
        ()
    }

    /** PROJ's answer on a thread of its own: an EPSG code needs `proj.db`, found through the search path in force. */
    private def onNewThread(body: => String): String = {
        var out = "ERROR the thread did not run"
        val t = new Thread(() => out = try body catch { case e: Throwable => s"ERROR ${e.getClass.getName}: ${e.getMessage}" })
        t.start()
        t.join()
        out
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
        val Array(first, second, root) = args
        try GDALManager.ensureJniLoaded(Some(first))
        catch {
            case e: Throwable =>
                println(s"FIRST=ERROR ${e.getClass.getName}: ${e.getMessage}")
                System.exit(3)
        }
        println(s"PRE=${onNewThread(transform())}")
        // GDAL fixes its data directory at the first file lookup: make that one before the first payload goes
        println(s"FIND_PRE=${Option(gdal.FindFile("gdal", "bag_template.xml")).getOrElse("<not found>")}")
        delete(new File(root))
        require(!Files.exists(Paths.get(first, "lib", "libgdalalljni.so")), "the first payload is still there")
        val later =
            try { GDALManager.ensureJniLoaded(Some(second)); "ok" }
            catch { case e: IllegalStateException => s"ERROR ${e.getMessage}" }
        println(s"LATER=$later")
        println(s"PROJ_DATA=${Option(gdal.GetConfigOption("PROJ_DATA")).getOrElse("<unset>")}")
        println(s"GDAL_DATA=${Option(gdal.GetConfigOption("GDAL_DATA")).getOrElse("<unset>")}")
        println(s"FIND=${Option(gdal.FindFile("gdal", "bag_template.xml")).getOrElse("<not found>")}")
        println(s"TRANSFORM=${onNewThread(transform())}")
        println(s"JNI=${GDALManager.ensureJniLoaded(Some(second)).jni}")
        println("DONE")
    }
}
