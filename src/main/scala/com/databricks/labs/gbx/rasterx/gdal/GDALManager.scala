package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.internal.Logging
import org.gdal.gdal.gdal

import java.io.File
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

/**
  * One-time GDAL environment setup for the JVM process (driver or executor).
  *
  * Initializes GDAL driver registration, config options (from ExpressionConfig), and shared native
  * libraries. Must be called before any raster operations; typically triggered from
  * [[com.databricks.labs.gbx.rasterx.functions.register]] or when the first raster expression
  * runs on an executor.
  *
  * Which GDAL: the JNI of the env wheel's payload named by [[NativeDirKey]], else by `GBX_NATIVE_ROOT`, else the
  * system's `/usr/lib/libgdalalljni.so` ([[resolveNative]]). One GDAL per JVM: a second one is refused.
  */
object GDALManager extends Logging {

    /**
      * Network-capable GDAL drivers that can trigger outbound HTTP at parse time.
      * Skipped by default (never registered); users can override via `spark.gdal.GDAL_SKIP`.
      */
    val DefaultSkippedDrivers = "WMS WMTS WCS WFS HTTP CSW OGCAPI"

    /** The env wheel's `_native/<arch>` directory (Python `_native.native_dir()`); its `lib/` holds the JNI. */
    val NativeDirKey = "spark.databricks.labs.gbx.native.dir"

    /** The GDAL a JVM runs: its JNI, and its PROJ and GDAL data (None: the system GDAL finds its own). */
    final case class NativeChoice(jni: String, projData: Option[String], gdalData: Option[String])

    private val JniName = "libgdalalljni.so"
    private val LegacyJni = s"/usr/lib/$JniName"
    private val PayloadJni = s"(.*/_native/(?:x86_64|aarch64))/lib/$JniName".r
    private val LibgdalFile = """.*/libgdal(-[0-9a-f]{8})?\.so(\.\d+)+""".r
    // CA stores, the first that exists wins (the payload's curl has no compiled-in CA path); as _native/_rules.py
    private val SystemCa = Seq("/etc/ssl/certs/ca-certificates.crt", "/etc/pki/tls/certs/ca-bundle.crt", "/etc/ssl/cert.pem")
    private val OneGdal = "Use one GDAL per cluster: the env wheel's or the system one, not both."

    var isEnabled = false
    @volatile private var loaded: Option[NativeChoice] = None
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
                    loadSharedObjects(config.getSharedObjects.values, config.nativeDir)
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

    /** Apply ExpressionConfig to GDAL options for this process.
      *
      * The synthetic key `spark.databricks.labs.gbx.gdal.PROJ_GRID_DIRS` (populated by
      * [[com.databricks.labs.gbx.expressions.ExpressionConfig.apply]] from
      * [[com.databricks.labs.gbx.operations.ProjGridRegistry]]) is handled separately:
      * it is EXCLUDED from the generic SetConfigOption loop (which would produce a bogus
      * `PROJ_GRID_DIRS` GDAL option) and instead used to PREPEND to `PROJ_DATA`/`PROJ_LIB`
      * so the in-JVM GDAL PROJ search path is extended.
      *
      * `gdal.Warp()` in [[com.databricks.labs.gbx.rasterx.operator.GDALWarp]] is the in-JVM
      * GDAL Java API — not a subprocess — so SetConfigOption("PROJ_DATA", …) reaches it
      * directly; no child-process environment patching is needed for the warp/transform path.
      */
    def configureGDAL(config: ExpressionConfig): Unit = {
        val CPL_TMPDIR = config.configs.getOrElse("cpl_tmpdir", "/tmp/gdal")
        val GDAL_PAM_PROXY_DIR = config.configs.getOrElse("gdal_pam_proxy_dir", "/tmp/gdal/pam")
        configureGDAL(CPL_TMPDIR, GDAL_PAM_PROXY_DIR)
        // Apply GDAL config options; skip PROJ_GRID_DIRS (synthetic GeoBrix key, handled below).
        config.getGDALConfig.foreach { case (key, value) =>
            val gdalKey = key
                .stripPrefix("spark.databricks.labs.gbx.gdal.")
                .stripPrefix("spark.gdal.")
            if (gdalKey != "PROJ_GRID_DIRS") gdal.SetConfigOption(gdalKey, value)
        }
        // Prepend user-supplied PROJ grid dirs to the in-JVM PROJ search path.
        config.configs.get("spark.databricks.labs.gbx.gdal.PROJ_GRID_DIRS").foreach { dirs =>
            val existing = Option(gdal.GetConfigOption("PROJ_DATA")).getOrElse("/usr/share/proj")
            val prepended = (dirs.split(":").toSeq :+ existing).distinct.mkString(":")
            gdal.SetConfigOption("PROJ_DATA", prepended)
            gdal.SetConfigOption("PROJ_LIB", prepended) // legacy alias PROJ still honors
        }
        // The payload's curl has no compiled-in CA path: GDAL gets the system store, unless the user named a bundle
        // (GDAL reads these three as config options or environment variables, in this order).
        if (loaded.exists(_.projData.isDefined) &&
            Seq("GDAL_CURL_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE").forall(k => gdal.GetConfigOption(k) == null)) {
            SystemCa.find(p => Files.isRegularFile(Paths.get(p))).foreach(gdal.SetConfigOption("GDAL_CURL_CA_BUNDLE", _))
        }
    }

    def configureGDAL(CPL_TMPDIR: String, GDAL_PAM_PROXY_DIR: String, CPL_DEBUG: String = "OFF",
                      logCPL: Boolean = false): Unit = {
        // Must be set BEFORE gdal.AllRegister() in init; skipped drivers are never registered.
        gdal.SetConfigOption("GDAL_SKIP", DefaultSkippedDrivers)
        // PROJ and GDAL data: the loaded payload's, else the system PROJ's data dir (the system GDAL finds GDAL_DATA).
        val choice = loaded.getOrElse(NativeChoice(LegacyJni, None, None))
        if (choice.projData.isEmpty) gdal.SetConfigOption("PROJ_LIB", "/usr/share/proj")
        applyNativeData(choice)
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

    /** Load the GDAL JNI ([[ensureJniLoaded]] with `nativeDir`, the [[NativeDirKey]] value), then any extra
      * shared objects the user named. */
    def loadSharedObjects(sharedObjects: Iterable[String], nativeDir: Option[String] = None): Unit = {
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
        ensureJniLoaded(nativeDir)
        sharedObjects.foreach(loadOrNoop)
    }

    /**
      * The GDAL this JVM should load, or why it must not load one. Pure: the file system and the process come in as
      * arguments (`exists`, `realPath`, the libgdal files the process maps, the JVM's library path).
      *
      * The JNI is `<confDir>/lib/libgdalalljni.so`, else `<envRoot>/_native/<arch>/lib/...` (`GBX_NATIVE_ROOT`), else
      * `/usr/lib/libgdalalljni.so` (or the first on the library path); a payload's data is `<dir>/../share/{proj,gdal}`,
      * also when the system JNI is a link into a payload (DCS, init script). Refused: a missing JNI; a libgdal mapped
      * from outside the chosen payload; another `libgdalalljni.so` first on the library path, because GDAL's Java
      * classes `System.loadLibrary` it by name on first use and would map a second GDAL.
      */
    def resolveNative(
        confDir: Option[String],
        envRoot: Option[String],
        osArch: String,
        exists: String => Boolean,
        realPath: String => String,
        mappedLibgdal: Seq[String],
        libraryPath: Seq[String] = Nil
    ): Either[String, NativeChoice] = {
        val onPath = libraryPath.map(d => s"${d.stripSuffix("/")}/$JniName").find(exists)
        val chosen = (confDir.filter(_.nonEmpty), envRoot.filter(_.nonEmpty)) match {
            case (Some(dir), _)     => payload(dir.stripSuffix("/"), NativeDirKey, exists)
            case (None, Some(root)) =>
                payloadArch(osArch)
                    .toRight(s"geobrix: GBX_NATIVE_ROOT is set, but the payload has no build for this JVM's architecture ($osArch)")
                    .flatMap(a => payload(s"${root.stripSuffix("/")}/_native/$a", "GBX_NATIVE_ROOT", exists))
            case (None, None)       =>
                Some(LegacyJni).filter(exists).orElse(onPath)
                    .toRight(
                      s"geobrix: no GDAL JNI: $LegacyJni is missing and java.library.path has none. Fix: set $NativeDirKey " +
                          "to the env wheel's _native/<arch> directory, or install GDAL on the cluster."
                    )
                    .map(jni =>
                        realPath(jni) match {
                            case PayloadJni(dir) => NativeChoice(jni, Some(s"$dir/../share/proj"), Some(s"$dir/../share/gdal"))
                            case _               => NativeChoice(jni, None, None)
                        }
                    )
        }
        chosen.flatMap { c =>
            val libDir = c.jni.stripSuffix(s"/$JniName")
            val foreign = if (c.projData.isDefined) foreignLibgdal(mappedLibgdal, c, realPath) else Nil
            val shadow = onPath.filter(p => realPath(p) != realPath(c.jni))
            if (foreign.nonEmpty) Left(differentGdal(foreign, libDir))
            else if (shadow.isDefined) Left(
              s"geobrix: a different GDAL is on this JVM's library path (${shadow.get}), and GDAL's Java classes load it " +
                  s"by name; expected one from $libDir. $OneGdal (Or put $libDir first on java.library.path.)"
            )
            else Right(c)
        }
    }

    /**
      * Load the GDAL JNI once per JVM (under the init lock) and return what was loaded. `confDir` is the
      * [[NativeDirKey]] value, None where the caller has no ExpressionConfig. Throws IllegalStateException with
      * [[resolveNative]]'s refusal, and when `confDir` names another JNI than the one already loaded.
      */
    def ensureJniLoaded(confDir: Option[String]): NativeChoice = loaded match {
        case Some(c) if confDir.forall(_.isEmpty) => c
        case _                                    =>
            lock.synchronized {
                loaded match {
                    case Some(c) =>
                        confDir.map(d => s"${d.stripSuffix("/")}/lib/$JniName").filter(j => realPathOf(j) != realPathOf(c.jni))
                            .foreach(j => throw new IllegalStateException(differentGdal(Seq(c.jni), j.stripSuffix(s"/$JniName"))))
                        c
                    case None    =>
                        val c = resolveNative(
                          confDir,
                          sys.env.get("GBX_NATIVE_ROOT"),
                          System.getProperty("os.arch", ""),
                          p => Files.exists(Paths.get(p)),
                          realPathOf,
                          mappedLibgdal(),
                          libraryPath()
                        ) match {
                            case Right(choice) => choice
                            case Left(refusal) => throw new IllegalStateException(refusal)
                        }
                        System.load(c.jni)
                        // GDAL's Java classes System.loadLibrary their JNI by name on first use: run that now, so the
                        // check below sees anything it mapped. With none on the library path they print "Native
                        // library load failed" to stderr and bind the JNI loaded above.
                        if (!org.gdal.gdal.gdalJNI.isAvailable()) {
                            logInfo(s"No $JniName on java.library.path; GDAL's Java classes use ${c.jni}")
                        }
                        val foreign = if (c.projData.isDefined) foreignLibgdal(mappedLibgdal(), c, realPathOf) else Nil
                        if (foreign.nonEmpty) throw new IllegalStateException(differentGdal(foreign, c.jni.stripSuffix(s"/$JniName")))
                        applyNativeData(c) // before any driver registers: GDAL fixes its data path at the first lookup
                        loaded = Some(c)
                        logInfo(s"GDAL JNI loaded from ${c.jni} (${gdal.VersionInfo("RELEASE_NAME")})")
                        c
                }
            }
    }

    /** [[ensureJniLoaded]] for the payload directory `dir`. */
    def loadNativeFrom(dir: String): Unit = ensureJniLoaded(Some(dir))

    private def payload(dir: String, source: String, exists: String => Boolean): Either[String, NativeChoice] = {
        val jni = s"$dir/lib/$JniName"
        if (exists(jni)) Right(NativeChoice(jni, Some(s"$dir/../share/proj"), Some(s"$dir/../share/gdal")))
        else Left(s"geobrix: no GDAL JNI at $jni (from $source, which must name the env wheel's native payload)")
    }

    private def payloadArch(osArch: String): Option[String] =
        osArch match {
            case "amd64" | "x86_64"  => Some("x86_64")
            case "aarch64" | "arm64" => Some("aarch64")
            case _                   => None
        }

    private def foreignLibgdal(mapped: Seq[String], c: NativeChoice, realPath: String => String): Seq[String] = {
        val lib = realPath(c.jni).stripSuffix(JniName)
        mapped.filterNot(p => realPath(p).startsWith(lib))
    }

    private def differentGdal(paths: Seq[String], libDir: String): String =
        s"geobrix: a different GDAL is already loaded in this process (${paths.mkString(", ")}); expected one from " +
            s"$libDir. $OneGdal"

    private def applyNativeData(c: NativeChoice): Unit = {
        c.projData.foreach { p =>
            gdal.SetConfigOption("PROJ_DATA", p)
            gdal.SetConfigOption("PROJ_LIB", p) // legacy alias PROJ still honors
        }
        c.gdalData.foreach(gdal.SetConfigOption("GDAL_DATA", _))
    }

    private def realPathOf(p: String): String = Try(Paths.get(p).toRealPath().toString).getOrElse(p)

    /** The libgdal files this process maps (Linux `/proc/self/maps`; empty elsewhere). */
    private def mappedLibgdal(): Seq[String] =
        Try(Files.readAllLines(Paths.get("/proc/self/maps")).asScala.toSeq).getOrElse(Nil)
            .flatMap(_.trim.split("\\s+", 6).lift(5)).filter(p => LibgdalFile.pattern.matcher(p).matches).distinct

    /** Where `System.loadLibrary` looks: the JDK's own library dir, then `java.library.path`. */
    private def libraryPath(): Seq[String] =
        Seq("sun.boot.library.path", "java.library.path").flatMap(k => Option(System.getProperty(k)))
            .flatMap(_.split(File.pathSeparator)).filter(_.nonEmpty)

}
