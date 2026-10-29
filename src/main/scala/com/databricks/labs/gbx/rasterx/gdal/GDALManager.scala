package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.internal.Logging
import org.gdal.gdal.gdal

import java.io.File
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.{Success, Try}

/**
  * One-time GDAL environment setup for the JVM process (driver or executor).
  *
  * Initializes GDAL driver registration, config options (from ExpressionConfig), and shared native
  * libraries. Must be called before any raster operations; typically triggered from
  * [[com.databricks.labs.gbx.rasterx.functions.register]] or when the first raster expression
  * runs on an executor.
  *
  * Which GDAL: the JNI of the env wheel's payload named by [[NativeDirKey]] (from the caller's ExpressionConfig, else
  * the session: [[sessionNativeDir]]), else by `GBX_NATIVE_ROOT`, else the system's `/usr/lib/libgdalalljni.so`
  * ([[resolveNative]]). One GDAL per JVM: a second one is refused, also when a later configuration names it.
  */
object GDALManager extends Logging {

    /**
      * The drivers the driver policy refuses (the manifest's `gdal.raster_drivers.refused` plus the OGR network drivers,
      * held equal by a test): stored
      * pipelines (GDALG), tile indexes and catalogs that open other rasters they name (GTI, STACTA, STACIT), and the
      * network APIs that can trigger outbound HTTP at open time. Skipped by default (never registered), so a nested
      * source cannot open them either; users can override via `spark.gdal.GDAL_SKIP`.
      */
    val DefaultSkippedDrivers = "GDALG GTI STACTA STACIT WMS WMTS WCS WFS HTTP CSW OGCAPI PLMOSAIC PLSCENES EEDAI EEDA DAAS NGW"

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
    private val FindDir =
        s"""set $NativeDirKey to the env wheel's native directory (python -c "from databricks.labs.gbx import """ +
            """_native; print(_native.native_dir())")"""

    var isEnabled = false
    @volatile private var loaded: Option[NativeChoice] = None
    // conf dirs already found to name the loaded JNI: a later init with one of them is a lock-free no-op
    @volatile private var sameAsLoaded: Set[String] = Set.empty
    // PROJ grid directories already on the PROJ search path, in the order applied (a union: none is dropped)
    @volatile private var appliedGridDirs: Seq[String] = Nil
    private val lock = new Object

    /** Tracks whether OGR drivers have been registered in this JVM. See [[initOgr]]. */
    @volatile private var ogrEnabled = false

    private val pythonLock = new Object
    @volatile private var pythonDepth = 0

    /** The one switch of the process-level driver policy (AP-43): `GBX_GDAL_DRIVER_POLICY=off` in the environment, or
      * `spark.databricks.labs.gbx.gdal.driver_policy=off` in the session conf (the same name the light tier reads).
      * Off: no refused driver is skipped and the raw-band setting is left as GDAL has it. Read at the first init. */
    val PolicyEnv = "GBX_GDAL_DRIVER_POLICY"
    val PolicyConf = "spark.databricks.labs.gbx.gdal.driver_policy"
    @volatile private var policyOn = true

    private def policyFrom(config: ExpressionConfig): Boolean = {
        val env = Option(System.getenv(PolicyEnv))
        val conf = config.configs.get(PolicyConf)
        !(env.exists(_.trim.equalsIgnoreCase("off")) || conf.exists(_.trim.equalsIgnoreCase("off")))
    }

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

    /** Initialize GDAL once per process; idempotent after first success. Every call checks that `config` names the
      * GDAL already loaded (a JVM cannot change GDAL) and puts any PROJ grid directories it brings that are not yet
      * on PROJ's search path there. */
    def init(config: ExpressionConfig): Unit = {
        ensureJniLoaded(config.nativeDir)
        lock.synchronized {
            if (!isEnabled) {
                Try {
                    policyOn = policyFrom(config)
                    loadSharedObjects(config.getSharedObjects.values, config.nativeDir)
                    configureGDAL(config)
                    gdal.AllRegister()
                    appliedGridDirs = gridDirs(config)
                    isEnabled = true
                } match {
                    case Success(_)                    => logInfo("GDAL environment enabled successfully.")
                    case scala.util.Failure(exception) =>
                        logError("GDAL not enabled. Rasterx requires that GDAL be installed on the cluster.")
                        logError(s"Error: ${exception.getMessage}")
                        isEnabled = false
                        throw exception
                }
            } else {
                val fresh = gridDirs(config).filterNot(appliedGridDirs.contains)
                if (fresh.nonEmpty) {
                    val current = Option(gdal.GetConfigOption("PROJ_DATA")).map(_.split(":").toSeq).getOrElse(Seq(baseProjData))
                    setProjSearchPath((fresh ++ appliedGridDirs ++ current).distinct)
                    appliedGridDirs = appliedGridDirs ++ fresh
                    logInfo(s"PROJ grid directories added to the search path: ${fresh.mkString(", ")}")
                }
            }
        }
    }

    private def gridDirs(config: ExpressionConfig): Seq[String] =
        config.configs.get("spark.databricks.labs.gbx.gdal.PROJ_GRID_DIRS").toSeq.flatMap(_.split(":")).filter(_.nonEmpty)

    private def baseProjData: String = loaded.flatMap(_.projData).getOrElse("/usr/share/proj")

    /** PROJ_DATA/PROJ_LIB for GDAL, and OSRSetPROJSearchPaths, which reaches PROJ contexts that already exist. */
    private def setProjSearchPath(dirs: Seq[String]): Unit = {
        gdal.SetConfigOption("PROJ_DATA", dirs.mkString(":"))
        gdal.SetConfigOption("PROJ_LIB", dirs.mkString(":")) // legacy alias PROJ still honors
        org.gdal.osr.osr.SetPROJSearchPaths(new java.util.Vector[String](dirs.asJava))
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
    def initOgr(): Unit = {
        ensureJniLoaded(None) // else GDAL's classes would load a JNI by name on their own
        lock.synchronized {
            if (!ogrEnabled) {
                org.gdal.ogr.ogr.RegisterAll()
                ogrEnabled = true
            }
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
        val dirs = gridDirs(config)
        if (dirs.nonEmpty) {
            val existing = Option(gdal.GetConfigOption("PROJ_DATA")).getOrElse("/usr/share/proj")
            setProjSearchPath((dirs ++ existing.split(":")).distinct)
        }
        // The payload's curl has no compiled-in CA path: GDAL gets the system store, unless the user named a bundle
        // (GDAL reads these three as config options or environment variables, in this order). This is _rules.py's
        // GDAL_CURL_CA_BUNDLE rule. Its PROJ_CURL_CA_BUNDLE half cannot be applied here: PROJ reads it only from the
        // process environment (or proj.ini's ca_bundle_path), GDAL forwards no CA to PROJ, and the Java bindings have
        // no proj_context_set_ca_bundle_path. PROJ's network is off by default; a JVM that turns it on
        // (PROJ_NETWORK=ON) needs PROJ_CURL_CA_BUNDLE in its environment (spark.executorEnv / the cluster's).
        if (loaded.exists(_.projData.isDefined) &&
            Seq("GDAL_CURL_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE").forall(k => gdal.GetConfigOption(k) == null)) {
            SystemCa.find(p => Files.isRegularFile(Paths.get(p))).foreach(gdal.SetConfigOption("GDAL_CURL_CA_BUNDLE", _))
        }
    }

    def configureGDAL(CPL_TMPDIR: String, GDAL_PAM_PROXY_DIR: String, CPL_DEBUG: String = "OFF",
                      logCPL: Boolean = false): Unit = {
        // Must be set BEFORE gdal.AllRegister() in init; skipped drivers are never registered. Refused drivers are
        // gone for every GDAL path of the JVM, nested sources included (AP-43); the switch turns that off.
        if (policyOn) gdal.SetConfigOption("GDAL_SKIP", DefaultSkippedDrivers)
        // PROJ and GDAL data: the loaded payload's, else the system PROJ's data dir (the system GDAL finds GDAL_DATA).
        val choice = loaded.getOrElse(NativeChoice(LegacyJni, None, None))
        if (choice.projData.isEmpty) gdal.SetConfigOption("PROJ_LIB", "/usr/share/proj")
        applyNativeData(choice)
        // a reset keeps the PROJ grid directories already applied in this JVM (a union: none is dropped)
        if (appliedGridDirs.nonEmpty) setProjSearchPath((appliedGridDirs :+ baseProjData).distinct)
        // Off by default; flipped on only around PixelCombineRasters.combine() via withVrtPython. A VRT's raw band reads
        // a file's bytes without a driver, so it is off for the process too (AP-43), with the switch on.
        if (policyOn) {
            gdal.SetConfigOption("GDAL_VRT_ENABLE_PYTHON", "NO")
            gdal.SetConfigOption("GDAL_VRT_ENABLE_RAWRASTERBAND", "NO")
        } else gdal.SetConfigOption("GDAL_VRT_ENABLE_PYTHON", "NO")
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
        val c = ensureJniLoaded(nativeDir)
        if (sharedObjects.nonEmpty) {
            // a user-named library may pull in its own libgdal: one the JNI load had not mapped is a second GDAL,
            // for the payload's and the system's GDAL alike
            val before = mappedLibgdal().map(realPathOf).toSet
            sharedObjects.foreach(loadOrNoop)
            val added = mappedLibgdal().filterNot(p => before.contains(realPathOf(p)))
            if (added.nonEmpty) throw new IllegalStateException(differentGdal(added, c.jni.stripSuffix(s"/$JniName")))
        }
    }

    /**
      * The GDAL this JVM should load, or why it must not load one. Pure: the file system and the process come in as
      * arguments (`exists`, `realPath`, the libgdal files the process maps, the JVM's library path).
      *
      * The JNI is `<confDir>/lib/libgdalalljni.so`, else `<envRoot>/_native/<arch>/lib/...` (`GBX_NATIVE_ROOT`), else
      * `/usr/lib/libgdalalljni.so` (or the first on the library path); a payload's data is `<dir>/../share/{proj,gdal}`,
      * also when the system JNI is a link into a payload (DCS, init script). Refused: a missing JNI; a libgdal mapped
      * from outside the chosen payload; another `libgdalalljni.so` first on the library path, because GDAL's Java
      * classes `System.loadLibrary` it by name on first use and would map a second GDAL. `jniIdentity` decides "another"
      * after `realPath` (a byte-identical copy of the same build is the same JNI).
      */
    def resolveNative(
        confDir: Option[String],
        envRoot: Option[String],
        osArch: String,
        exists: String => Boolean,
        realPath: String => String,
        mappedLibgdal: Seq[String],
        libraryPath: Seq[String] = Nil,
        jniIdentity: String => String = identity
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
                      s"geobrix: no GDAL JNI: $NativeDirKey and GBX_NATIVE_ROOT are unset, and $LegacyJni is missing. " +
                          s"Fix: install the GeoBrix env wheel and $FindDir."
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
            val shadow = onPath.filterNot(p => sameJni(p, c.jni, realPath, jniIdentity))
            if (foreign.nonEmpty) Left(differentGdal(foreign, libDir))
            else if (shadow.isDefined) Left(
              s"geobrix: a different GDAL is on this JVM's library path (${shadow.get}), and GDAL's Java classes load it " +
                  s"by name; expected one from $libDir. $OneGdal Fix: remove that GDAL from the cluster, or make " +
                  s"${shadow.get} a link to $libDir/$JniName."
            )
            else Right(c)
        }
    }

    private def sameJni(a: String, b: String, realPath: String => String, jniIdentity: String => String): Boolean = {
        val (ra, rb) = (realPath(a), realPath(b))
        ra == rb || jniIdentity(ra) == jniIdentity(rb)
    }

    /**
      * Load the GDAL JNI once per JVM (under the init lock) and return what was loaded. `confDir` is the
      * [[NativeDirKey]] value of the caller's ExpressionConfig; None (no ExpressionConfig: KernelGdal, the vector
      * writers, OGR) uses the session's ([[sessionNativeDir]]). Throws IllegalStateException with
      * [[resolveNative]]'s refusal, and when `confDir` names another JNI than the one already loaded (a JVM cannot
      * unload a GDAL).
      */
    def ensureJniLoaded(confDir: Option[String]): NativeChoice = loaded match {
        case Some(c) if confDir.forall(d => d.isEmpty || sameAsLoaded.contains(d)) => c
        case _                                                                      =>
            lock.synchronized {
                loaded match {
                    case Some(c) =>
                        confDir.filter(_.nonEmpty).foreach { d =>
                            val j = s"${d.stripSuffix("/")}/lib/$JniName"
                            if (!Files.exists(Paths.get(j))) {
                                throw new IllegalStateException(
                                  s"geobrix: no GDAL JNI at $j (from $NativeDirKey); this JVM runs the GDAL loaded from " +
                                      s"${c.jni}. Fix: $FindDir."
                                )
                            }
                            if (!sameJni(j, c.jni, realPathOf, jniIdentityOf)) throw new IllegalStateException(laterGdal(c.jni, j))
                            sameAsLoaded += d
                        }
                        c
                    case None    =>
                        val dir = confDir.filter(_.nonEmpty).orElse(sessionNativeDir())
                        val env = sys.env.get("GBX_NATIVE_ROOT").filter(_.nonEmpty)
                        val c = resolveNative(
                          dir,
                          env,
                          System.getProperty("os.arch", ""),
                          p => Files.exists(Paths.get(p)),
                          realPathOf,
                          mappedLibgdal(),
                          libraryPath(),
                          jniIdentityOf
                        ) match {
                            case Right(choice) => choice
                            case Left(refusal) => throw new IllegalStateException(refusal)
                        }
                        if (dir.isEmpty && env.isEmpty) {
                            logInfo(s"No $NativeDirKey and no GBX_NATIVE_ROOT: this JVM runs the system GDAL (${c.jni})")
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
                        val version = bindCheck(c)
                        applyNativeData(c) // before any driver registers: GDAL fixes its data path at the first lookup
                        loaded = Some(c)
                        dir.foreach(d => sameAsLoaded += d)
                        logInfo(s"GDAL JNI loaded from ${c.jni} ($version)")
                        c
                }
            }
    }

    /** The GDAL release, once GDAL's Java classes have called into the JNI just loaded (their constants class reads
      * every constant from it). They cannot when they are another GDAL version than the JNI, or when another class
      * loader holds them (a second gdal jar on the class path): a clear refusal instead of a later link error. */
    private def bindCheck(c: NativeChoice): String =
        try {
            val v = gdal.VersionInfo("RELEASE_NAME")
            // reading one constant initializes the class, which reads every constant from the JNI
            val byteType = org.gdal.gdalconst.gdalconstConstants.GDT_Byte
            logDebug(s"GDAL $v: GDAL's Java classes reach the JNI (GDT_Byte = $byteType)")
            v
        } catch {
            case e: UnsatisfiedLinkError =>
                throw new IllegalStateException(
                  s"geobrix: GDAL's Java classes cannot use the GDAL loaded from ${c.jni} (${e.getMessage}): they are " +
                      "another GDAL version than that JNI, or another class loader loaded them. Fix: use the GDAL " +
                      "classes the GeoBrix JAR bundles, with the env wheel's GDAL, and no other gdal jar on the class path."
                )
        }

    /**
      * The session's [[NativeDirKey]] for a caller with no ExpressionConfig: on an executor the task's local property
      * (Spark SQL hands a task the session's `spark.*` settings), on the driver the active session's conf. Spark SQL
      * is reached only here, and its absence (UC Scala) is no error.
      */
    private[gdal] def sessionNativeDir(): Option[String] = {
        val task = Option(org.apache.spark.TaskContext.get()).flatMap(t => Option(t.getLocalProperty(NativeDirKey)))
        def session: Option[String] =
            try SessionConf.get(NativeDirKey)
            catch {
                case _: LinkageError => None
                case NonFatal(_)     => None
            }
        task.orElse(session).filter(_.nonEmpty)
    }

    /** Kept apart so `GDALManager` loads no Spark SQL class until a no-config load asks for the session's conf. */
    private object SessionConf {
        def get(key: String): Option[String] =
            org.apache.spark.sql.SparkSession.getActiveSession
                .orElse(org.apache.spark.sql.SparkSession.getDefaultSession)
                .flatMap(_.conf.getOption(key))
    }

    private def laterGdal(loadedJni: String, requested: String): String =
        s"geobrix: this JVM already runs the GDAL loaded from $loadedJni, and a later configuration names $requested; " +
            s"a JVM cannot unload a GDAL. Fix: set $NativeDirKey before the first GDAL use in this JVM (a new session " +
            s"or cluster). $OneGdal"

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

    /** Mapped libgdal files outside the chosen JNI's directory. By real path, not content: a mapped libgdal file is
      * one the dynamic linker opened, and a copy of the same build cannot add one (its JNI NEEDs the same hashed
      * SONAME, which the linker reuses once loaded), so a second file here is a second GDAL. */
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

    /** A JNI's identity beyond its path: its bytes' sha256 (1.5 MB, read only when two real paths differ). */
    private def jniIdentityOf(p: String): String =
        Try(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(Paths.get(p))).map("%02x".format(_)).mkString)
            .getOrElse(p)

    /** The libgdal files this process maps (Linux `/proc/self/maps`; empty elsewhere). */
    private def mappedLibgdal(): Seq[String] =
        Try(Files.readAllLines(Paths.get("/proc/self/maps")).asScala.toSeq).getOrElse(Nil)
            .flatMap(_.trim.split("\\s+", 6).lift(5)).filter(p => LibgdalFile.pattern.matcher(p).matches).distinct

    /** Where `System.loadLibrary` looks: the JDK's own library dir, then `java.library.path`. */
    private def libraryPath(): Seq[String] =
        Seq("sun.boot.library.path", "java.library.path").flatMap(k => Option(System.getProperty(k)))
            .flatMap(_.split(File.pathSeparator)).filter(_.nonEmpty)

}
