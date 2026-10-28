package com.databricks.labs.gbx.kernels

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration

import scala.util.control.NonFatal

/** GDAL init for facades that run without a driver-built ExpressionConfig (Java UDFs, UC Scala).
  * Uses the case-class constructor, not the companion `apply(spark)`, so no SparkSession or Catalyst type loads. */
object KernelGdal {
    @volatile private var done = false

    def ensureInit(): Unit = if (!done) synchronized {
        if (!done) {
            GDALManager.init(new ExpressionConfig(Map.empty, new SerializableConfiguration(new Configuration(false))))
            done = true
        }
    }

    /** `"ok:<GDAL release>"`, or `"error:<class>:<message>"` when the natives do not load. */
    def probe(): String =
        try {
            ensureInit()
            "ok:" + org.gdal.gdal.gdal.VersionInfo("RELEASE_NAME")
        } catch {
            case NonFatal(e)               => "error:" + e.getClass.getSimpleName + ":" + e.getMessage
            case e: UnsatisfiedLinkError   => "error:UnsatisfiedLinkError:" + e.getMessage
            case e: NoClassDefFoundError   => "error:NoClassDefFoundError:" + e.getMessage
        }
}
