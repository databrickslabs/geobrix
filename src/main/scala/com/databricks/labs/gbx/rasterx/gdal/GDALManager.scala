package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.internal.Logging
import org.gdal.gdal.gdal

import java.nio.file.{Files, Paths}
import scala.util.{Success, Try}

/**
  * One-time GDAL environment setup for the JVM process (driver or executor).
  *
  * Initializes GDAL driver registration, config options (from ExpressionConfig), shared native
  * libraries, and checkpoint paths. Must be called before any raster operations; typically
  * triggered from [[com.databricks.labs.gbx.rasterx.functions.register]] or when the first
  * raster expression runs on an executor.
  */
object GDALManager extends Logging {

    var isEnabled = false
    private val lock = AnyRef
    var checkpointPath: String = _
    var useCheckpoint: Boolean = _

    /** Initialize GDAL once per process; idempotent after first success. */
    def init(config: ExpressionConfig): Unit =
        lock.synchronized {
            if (!isEnabled) {
                Try {
                    loadSharedObjects(config.getSharedObjects.values)
                    configureGDAL(config)
                    gdal.AllRegister()
                    isEnabled = true
                } match {
                    case Success(_)                    => logInfo("GDAL environment enabled successfully.")
                    case scala.util.Failure(exception) =>
                        logError("GDAL not enabled. Rasterx requires that GDAL be installed on the cluster.")
                        logError(s"Error: ${exception.getMessage}")
                        isEnabled = false
                        throw exception
                }
            }
        }

    /** Apply ExpressionConfig to GDAL options and store checkpoint settings for this process. */
    def configureGDAL(config: ExpressionConfig): Unit = {
        val CPL_TMPDIR = config.configs.getOrElse("cpl_tmpdir", "/tmp/gdal")
        val GDAL_PAM_PROXY_DIR = config.configs.getOrElse("gdal_pam_proxy_dir", "/tmp/gdal/pam")
        configureGDAL(CPL_TMPDIR, GDAL_PAM_PROXY_DIR)
        config.getGDALConfig.foreach { case (key, value) => gdal.SetConfigOption(key, value) }
        this.checkpointPath = config.getRasterCheckpointDir
        this.useCheckpoint = config.useCheckpoint
    }

    def configureGDAL(CPL_TMPDIR: String, GDAL_PAM_PROXY_DIR: String, CPL_DEBUG: String = "OFF",
                      logCPL: Boolean = false): Unit = {
        gdal.SetConfigOption("PROJ_LIB", "/usr/share/proj")
        gdal.SetConfigOption("GDAL_VRT_ENABLE_PYTHON", "YES")
        gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "YES")
        gdal.SetConfigOption("CPL_TMPDIR", CPL_TMPDIR)
        gdal.SetConfigOption("GDAL_PAM_PROXY_DIR", GDAL_PAM_PROXY_DIR)
        gdal.SetConfigOption("GDAL_PAM_ENABLED", "YES")
        gdal.SetConfigOption("CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "NO")
        gdal.SetConfigOption("GDAL_CACHEMAX", "512")
        gdal.SetCacheMax(512 * 1024 * 1024)
        gdal.SetConfigOption("GDAL_NUM_THREADS", "4")
        // Option: Suppress PROJ CRS lookup warnings (non-critical warnings during reprojection)
        // Note: PROJ "crs not found" warnings cannot be suppressed via PushErrorHandler in Scala
        // due to GDAL Java bindings limitations. These warnings are non-critical and don't affect functionality.
        if (logCPL) {
          gdal.SetConfigOption("CPL_LOG", s"$CPL_TMPDIR/gdal.log")
        } else {
          gdal.SetConfigOption("CPL_LOG", "/dev/null")
        }
        gdal.SetConfigOption("CPL_DEBUG", CPL_DEBUG)
    }

    def loadSharedObjects(sharedObjects: Iterable[String]): Unit = {
        def loadOrNoop(path: String): Unit = {
            Try {
                if (Files.exists(Paths.get(path))) System.load(path)
            } match {
                case Success(_)                    => logInfo(s"Loaded GDAL shared object: $path")
                case scala.util.Failure(exception) =>
                    logError(s"Failed to load GDAL shared object: $path")
                    logError(s"Error: ${exception.getMessage}")
            }
        }
        loadOrNoop("/usr/lib/libgdalalljni.so")
        sharedObjects.foreach(loadOrNoop)
    }

}
