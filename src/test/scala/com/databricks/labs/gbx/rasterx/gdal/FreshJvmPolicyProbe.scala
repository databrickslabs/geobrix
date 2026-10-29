package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.gdal

/** The child JVM of [[GDALManagerPolicyTest]]: one fresh process whose first GDAL use is the one named, then what the
  * process's GDAL reports. Arguments: `ogr` (the first use is `initOgr`) or `raster` (`init`), then `key=value` pairs
  * for the config of an `init`. Prints `GDAL_SKIP=`, `GDAL_VRT_ENABLE_RAWRASTERBAND=`, `PRESENT=` (the refused drivers
  * GDAL has registered) and `DONE`.
  */
object FreshJvmPolicyProbe {

    private def show(key: String): String = Option(gdal.GetConfigOption(key)).getOrElse("<unset>")

    def main(args: Array[String]): Unit = {
        val mode = args(0)
        val configs = args.drop(1).map { kv =>
            val i = kv.indexOf('=')
            kv.take(i) -> kv.drop(i + 1)
        }.toMap
        try {
            if (mode == "ogr") GDALManager.initOgr()
            else GDALManager.init(ExpressionConfig(configs, new SerializableConfiguration(new Configuration(false))))
        } catch {
            case e: Throwable =>
                println(s"ERROR ${e.getClass.getName}: ${e.getMessage}")
                System.exit(3)
        }
        println(s"GDAL_SKIP=${show("GDAL_SKIP")}")
        println(s"GDAL_VRT_ENABLE_RAWRASTERBAND=${show("GDAL_VRT_ENABLE_RAWRASTERBAND")}")
        val present = GDALManager.DefaultSkippedDrivers.split(" ").filter(n => gdal.GetDriverByName(n) != null)
        println(s"PRESENT=${present.mkString(",")}")
        println("DONE")
    }
}
