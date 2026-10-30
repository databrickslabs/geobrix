package com.databricks.labs.gbx.rasterx.gdal

import java.io.File
import java.nio.file.{Files, Paths}
import org.gdal.gdal.gdal

/** The child JVM of [[GDALManagerStaleNativeDirTest]]: loads the GDAL JNI of payload directory `args(0)`, deletes that
  * directory (a notebook's Python environment, torn down when its run ends), then names the byte-identical payload
  * `args(1)` the way the next run's expressions do. Prints `LATER=ok` or `LATER=ERROR <message>`, then `PROJ_DATA=`,
  * `JNI=` (the loaded JNI path) and `DONE`.
  */
object StaleNativeDirProbe {

    /** Removes the tree, links as links: a link to a directory is not entered (its target is the staged payload's). */
    private def delete(f: File): Unit = {
        if (!Files.isSymbolicLink(f.toPath)) Option(f.listFiles()).foreach(_.foreach(delete))
        f.delete()
        ()
    }

    def main(args: Array[String]): Unit = {
        val Array(first, second) = args
        try GDALManager.ensureJniLoaded(Some(first))
        catch {
            case e: Throwable =>
                println(s"FIRST=ERROR ${e.getClass.getName}: ${e.getMessage}")
                System.exit(3)
        }
        delete(new File(first))
        require(!Files.exists(Paths.get(first, "lib", "libgdalalljni.so")), "the first payload is still there")
        val later =
            try { GDALManager.ensureJniLoaded(Some(second)); "ok" }
            catch { case e: IllegalStateException => s"ERROR ${e.getMessage}" }
        println(s"LATER=$later")
        println(s"PROJ_DATA=${Option(gdal.GetConfigOption("PROJ_DATA")).getOrElse("<unset>")}")
        println(s"JNI=${GDALManager.ensureJniLoaded(Some(second)).jni}")
        println("DONE")
    }
}
