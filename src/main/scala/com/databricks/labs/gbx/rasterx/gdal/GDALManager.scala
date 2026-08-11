package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.internal.Logging
import org.gdal.gdal.gdal

import java.nio.file.{Files, Paths}
import scala.util.{Success, Try}

/**
  * One-time GDAL environment setup for the JVM process (driver or executor).
  *
  * Initializes GDAL driver registration, config options (from ExpressionConfig), and shared native
  * libraries. Must be called before any raster operations; typically triggered from
  * [[com.databricks.labs.gbx.rasterx.functions.register]] or when the first raster expression
  * runs on an executor.
  */
object GDALManager extends Logging {

    /**
      * Network-capable GDAL drivers that can trigger outbound HTTP at parse time.
      * Skipped by default (never registered); users can override via `spark.gdal.GDAL_SKIP`.
      */
    val DefaultSkippedDrivers = "WMS WMTS WCS WFS HTTP CSW OGCAPI"

    var isEnabled = false
    private val lock = AnyRef

    /** Tracks whether OGR drivers have been registered in this JVM. See [[initOgr]]. */
    @volatile private var ogrEnabled = false

    private val pythonLock = new Object
    @volatile private var pythonDepth = 0

    /**
      * Bracket `block` with `GDAL_VRT_ENABLE_PYTHON=YES` so a VRT with a `<PixelFunctionLanguage>Python</...>`
      * band can evaluate, then reset to `NO`. Safe for concurrent Spark tasks in the same JVM via a
      * reference count (GDAL Java bindings expose only process-global `SetConfigOption`).
      */
    def withVrtPython[T](block: => T): T = {
        pythonLock.synchronized {
            if (pythonDepth == 0) gdal.SetConfigOption("GDAL_VRT_ENABLE_PYTHON", "YES")
            pythonDepth += 1
        }
        try block
        finally pythonLock.synchronized {
            pythonDepth -= 1
            if (pythonDepth == 0) gdal.SetConfigOption("GDAL_VRT_ENABLE_PYTHON", "NO")
        }
    }

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

    /**
      * Register OGR (vector) drivers once per JVM; idempotent after first success.
      *
      * The GDAL Java bindings expose `org.gdal.ogr.ogr.RegisterAll()` as a process-global
      * mutation of the driver registry, and it is NOT thread-safe. Concurrent Spark tasks in
      * one executor JVM that each call `RegisterAll()` ad-hoc race that registry — a racing
      * `GetDriverByName(...)` can return null (NPE) or the native layer can sigabrt. Registering
      * exactly once under the shared `lock` (mirroring [[init]]) closes that race: after the
      * first call every task sees a fully-registered registry and never re-registers.
      */
    def initOgr(): Unit =
        lock.synchronized {
            if (!ogrEnabled) {
                org.gdal.ogr.ogr.RegisterAll()
                ogrEnabled = true
            }
        }

    /**
      * Returns the GTiff driver via a guarded lookup (REQUIRED for thread-safety).
      *
      * The GDAL Java bindings expose `gdal.GetDriverByName` against the process-global driver
      * registry. A raw per-task lookup can race [[init]]'s `AllRegister()` and return null
      * (NPE) or sigabrt the executor. This accessor takes the same `lock` [[init]]/[[initOgr]]
      * use, so the driver registry is fully populated before the lookup returns. Callers must
      * have already ensured [[init]] ran (raster expressions always do).
      */
    private[rasterx] def gtiffDriver(): org.gdal.gdal.Driver = driverByName("GTiff")

    /** Returns the in-memory MEM driver via the same guarded lookup as [[gtiffDriver]]. */
    private[rasterx] def memDriver(): org.gdal.gdal.Driver = driverByName("MEM")

    /** Guarded `gdal.GetDriverByName` — see [[gtiffDriver]] for the thread-safety rationale. */
    private def driverByName(shortName: String): org.gdal.gdal.Driver =
        lock.synchronized {
            gdal.GetDriverByName(shortName)
        }

    /** Apply ExpressionConfig to GDAL options for this process. */
    def configureGDAL(config: ExpressionConfig): Unit = {
        val CPL_TMPDIR = config.configs.getOrElse("cpl_tmpdir", "/tmp/gdal")
        val GDAL_PAM_PROXY_DIR = config.configs.getOrElse("gdal_pam_proxy_dir", "/tmp/gdal/pam")
        configureGDAL(CPL_TMPDIR, GDAL_PAM_PROXY_DIR)
        config.getGDALConfig.foreach { case (key, value) =>
            val gdalKey = key
                .stripPrefix("spark.databricks.labs.gbx.gdal.")
                .stripPrefix("spark.gdal.")
            gdal.SetConfigOption(gdalKey, value)
        }
    }

    def configureGDAL(CPL_TMPDIR: String, GDAL_PAM_PROXY_DIR: String, CPL_DEBUG: String = "OFF",
                      logCPL: Boolean = false): Unit = {
        // Must be set BEFORE gdal.AllRegister() in init; skipped drivers are never registered.
        gdal.SetConfigOption("GDAL_SKIP", DefaultSkippedDrivers)
        gdal.SetConfigOption("PROJ_LIB", "/usr/share/proj")
        // Off by default; flipped on only around PixelCombineRasters.combine() via withVrtPython.
        gdal.SetConfigOption("GDAL_VRT_ENABLE_PYTHON", "NO")
        gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "YES")
        gdal.SetConfigOption("CPL_TMPDIR", CPL_TMPDIR)
        gdal.SetConfigOption("GDAL_PAM_PROXY_DIR", GDAL_PAM_PROXY_DIR)
        // GDAL writes intermediate files (e.g. the COG driver's .ovr.tmp overview
        // sidecar) into CPL_TMPDIR and PAM sidecars into GDAL_PAM_PROXY_DIR. GDAL
        // does not create these dirs itself — a missing dir makes VSI_TIFFOpen()
        // fail and gdal.Translate return null (see rst_cog_convert). Create them.
        Try(Files.createDirectories(Paths.get(CPL_TMPDIR)))
        Try(Files.createDirectories(Paths.get(GDAL_PAM_PROXY_DIR)))
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
