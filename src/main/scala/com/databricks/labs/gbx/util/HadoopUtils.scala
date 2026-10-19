package com.databricks.labs.gbx.util

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.fs._
import org.apache.orc.util.Murmur3
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import scala.collection.mutable
import scala.util.control.NonFatal

/** Path normalization (Volumes, DBFS, file:), listing, first-file, and copy for Hadoop filesystems. */
//noinspection ScalaWeakerAccess
object HadoopUtils extends Logging {

    /** Shared schema-divergence error message for multi-stem shapefile directories.
      * Wording is identical across the heavy (Scala) and light (Python) tiers so the
      * user sees a consistent message regardless of which reader surfaces the error. */
    def shapefileDivergenceMsg(path: String, stemA: String, stemB: String): String =
        s"shapefile reader: shapefiles under $path have differing schemas; " +
        s"load them separately or use a single-stem directory. Stems: $stemA, $stemB."

    var hadoopConf: SerializableConfiguration = _

    /** Sets the default Hadoop config used by listHadoopFiles when no config is passed. */
    def setHadoopConf(hconf: SerializableConfiguration): Unit = {
        hadoopConf = hconf
    }

    /** Normalizes a path to a Hadoop-resolvable form for the supported DBR storage fabric.
      *
      * Unity Catalog Volumes (`/Volumes/...`) are kept BARE — no scheme — so Hadoop resolves them
      * through Databricks' UC-integrated connector, which carries the Volume credential on executors.
      * Adding a `file:` scheme forces `LocalFileSystem` (the raw FUSE mount), which is NOT credentialed
      * in opaque DataSource-V2 executor tasks and fails with "Operation not permitted" / FileNotFound.
      * (Validated: released 0.3.0 keeps `/Volumes` bare and reads it cleanly on current DBRs; the
      * `file:` rewrite was the 0.4.0 regression that broke executor reads.) The `dbfs:/Volumes/...`,
      * `/dbfs/Volumes/...` (note the plural — never `/Volume/`), and legacy `file:/Volumes/...` forms
      * all normalize to the same bare `/Volumes/...`.
      *
      * Workspace files (`/Workspace/...`) use the `file:` connector. Legacy pre-Volumes DBFS
      * (`/dbfs/...`, `dbfs:/...`) is NOT a supported target and is coerced off the `/dbfs` prefix
      * (best-effort `file:` — such reads are expected to fail rather than silently use the retired mount).
      *
      *  - `/Volumes/...`, `dbfs:/Volumes/...`, `/dbfs/Volumes/...`, `file:/Volumes/...` -> `/Volumes/...`
      *  - `/Workspace/...` -> `file:/Workspace/...`; other `file:/...` kept as-is
      *  - `/tmp/...` and other OS-absolute paths -> `file:...`
      */
    def cleanPath(inPath: String): String = {
        inPath match {
            // Unity Catalog Volumes -> BARE /Volumes/... (UC connector, credentialed on executors).
            // A file: scheme would force LocalFileSystem (raw FUSE) -> EPERM in DSV2 executor tasks.
            case _ if inPath.startsWith("/Volumes/")       => inPath
            case _ if inPath.startsWith("dbfs:/Volumes/")  => inPath.stripPrefix("dbfs:")
            case _ if inPath.startsWith("/dbfs/Volumes/")  => inPath.stripPrefix("/dbfs")
            case _ if inPath.startsWith("file:/Volumes/")  => inPath.stripPrefix("file:")
            // Workspace files FUSE -> file:/Workspace/...
            case _ if inPath.startsWith("/Workspace/")     => s"file:$inPath"
            // Already a local/FUSE connector URI (file:/Workspace/, file:/tmp/, ...).
            case _ if inPath.startsWith("file:/")          => inPath
            // Legacy pre-Volumes DBFS is unsupported: coerce AWAY from /dbfs and the dbfs: connector
            // (neither is used on supported DBRs). No automatic mapping to /Volumes or /Workspace.
            case _ if inPath.startsWith("/dbfs/")          => s"file:${inPath.stripPrefix("/dbfs")}"
            case _ if inPath.startsWith("dbfs:/")          => s"file:/${inPath.stripPrefix("dbfs:/")}"
            // OS-absolute local paths (incl. /tmp/).
            case _ if inPath.startsWith("/")               => s"file:$inPath"
            case _                                         => s"file:/$inPath"
        }
    }

    /** Lists non-directory files under inPath using hadoopConf. */
    def listHadoopFiles(inPath: String): Seq[String] = {
        listHadoopFiles(inPath, hadoopConf)
    }

    /** Spark/Hadoop convention: a name starting with '_' or '.' is a marker or hidden file
      * (e.g. `_SUCCESS`, `_committed_*`, `.crc`), NOT data. Skip these when enumerating a
      * directory so a reader pointed at a writer's output dir doesn't try to open `_SUCCESS`
      * as a dataset (mirrors Spark's default PathFilter). */
    private def isDataFile(path: Path): Boolean = {
        val n = path.getName
        !n.startsWith("_") && !n.startsWith(".")
    }

    /** Returns true for known Spark/Hadoop job-marker files that should always be excluded from
      * data listings: `_SUCCESS`, `_metadata`, `_common_metadata`, `_committed_*`, `_started_*`,
      * and CRC sidecar files. Does NOT exclude all `_`-prefixed files — this is intentionally
      * different from the Spark FileScan hidden-file filter, which excludes ALL `_`-prefixed names
      * and therefore misses legitimate data files like `_us_roads.shp`. */
    private def isMarkerFile(path: Path): Boolean = {
        val n = path.getName
        n == "_SUCCESS" || n == "_metadata" || n == "_common_metadata" ||
        n.startsWith("_committed_") || n.startsWith("_started_") || n.endsWith(".crc")
    }

    /** Lists non-directory data files under inPath using the given Hadoop config
      * (marker/hidden files like `_SUCCESS` are skipped). */
    def listHadoopFiles(inPath: String, hconf: SerializableConfiguration): Seq[String] = {
        val path = new Path(new URI(cleanPath(inPath)))
        withFileSystem(path, hconf) { fs =>
            fs.listStatus(path)
                .filter(st => !st.isDirectory && isDataFile(st.getPath))
                .map(_.getPath.toString)
                .toSeq
        }
    }

    /** Returns the first data file (by listing order) under inPath; used for schema inference
      * from a single file. Marker/hidden files (`_SUCCESS`, `.crc`, ...) are skipped so a
      * directory of writer output infers schema from a real shard, not the `_SUCCESS` marker. */
    def getFirstFile(inPath: String, hconf: SerializableConfiguration): String = {
        val path = new Path(new URI(cleanPath(inPath)))
        withFileSystem(path, hconf) { fs =>
            val status = fs.getFileStatus(path)
            if (status.isDirectory) {
                val it = fs.listFiles(path, false)
                var first: String = null
                while (first == null && it.hasNext) {
                    val st = it.next()
                    if (isDataFile(st.getPath)) first = st.getPath.toString
                }
                if (first == null) {
                    throw new IllegalArgumentException(s"No data files found under directory: $inPath")
                }
                first
            } else {
                path.toString
            }
        }
    }

    // OGR shapefile companion ("sidecar") extensions: a .shp dataset is a directory bundle of
    // .shp + these. Only the .shp is openable by the driver; the rest are staged alongside it.
    private val OGR_SIDECAR_EXTS: Set[String] =
        Set("cpg", "dbf", "prj", "shx", "qpj", "sbn", "sbx", "qix", "fbn", "fbx",
            "ain", "aih", "ixs", "mxs", "atx", "cst")

    private def fileExt(p: String): String = {
        val n = p.toLowerCase(java.util.Locale.ROOT)
        val slash = math.max(n.lastIndexOf('/'), n.lastIndexOf('\\'))
        val dot = n.lastIndexOf('.')
        if (dot > slash) n.substring(dot + 1) else ""
    }

    /** Picks the PRIMARY data file from a listing for schema inference — the file the OGR driver
      * actually opens, NOT a sidecar. A shapefile directory lists (sorted) as
      * `[.cpg, .dbf, .prj, .shp, .shx]`; `files.head` would be `.cpg`, which OGR cannot open as an
      * ESRI Shapefile (it threw `Could not open dataset ... at .../<name>.cpg`). Excluding the known
      * shapefile sidecars makes `.shp` the head; non-shapefile datasets (`.geojson`, `.gpkg`, ...)
      * have no matching sidecars and are unaffected. `stageHeadForSchemaSpark` still stages the
      * stem-siblings alongside, so the driver sees the full bundle. */
    def primaryDataFile(files: Seq[String]): Option[String] =
        files.find(p => !OGR_SIDECAR_EXTS.contains(fileExt(p))).orElse(files.headOption)

    /** Filters a file listing to primary data files, excluding OGR shapefile sidecar extensions
      * (`.dbf`, `.shx`, `.prj`, `.cpg`, etc.) that the ESRI Shapefile driver opens as
      * attribute-only datasources. All non-sidecar files (`.shp`, `.zip`, `.geojson`, `.gpkg`,
      * etc.) are kept — this supports multi-stem directories and heterogeneous driver mixes.
      * Used in the OGR batch read path to prevent loose sidecars from being double-counted. */
    def primaryDataFiles(files: Seq[String]): Seq[String] =
        files.filterNot(p => OGR_SIDECAR_EXTS.contains(fileExt(p)))

    /** Lists data files under inPath via Spark's file index, which forwards the UC Volume / WSFS
      * credential. This is for the ANALYZER thread only: the OGR reader's `inferSchema` runs on the
      * Spark analyzer thread, where a raw Hadoop FS `getFileStatus`/`listStatus` does NOT carry the
      * UC credential and throws `FileNotFoundException` on `/Volumes`; Spark's file index does carry
      * it. (Executor reads are separate — they use the bare `/Volumes` UC connector via copyToPath;
      * do NOT extend this Spark-listing workaround to executor reads or reintroduce `file:` paths.)
      * A single file or a `.gdb` / `.gdb.zip` / `.zip` dataset is returned as-is (not a dir of
      * shards). Falls back to the raw Hadoop FS listing if Spark's binaryFile can't enumerate it.
      * When binaryFile returns empty (Spark's hidden-file filter skips ALL `_`-prefixed names),
      * falls back to a raw Hadoop FS listing via an executor task, which carries the UC credential
      * and has no `_`-prefix filter — stripping only known Spark/Hadoop job-marker files. */
    def listDataFilesSpark(spark: org.apache.spark.sql.SparkSession, inPath: String): Seq[String] = {
        val cp = cleanPath(inPath)
        val lower = inPath.toLowerCase(java.util.Locale.ROOT).stripSuffix("/")
        if (lower.endsWith(".gdb") || lower.endsWith(".gdb.zip") || lower.endsWith(".zip")) {
            Seq(cp)
        } else {
            try {
                val files = spark.read.format("binaryFile").load(cp).inputFiles.toSeq.sorted
                if (files.nonEmpty) {
                    files
                } else {
                    // binaryFile returned empty: Spark's hidden-file filter may have skipped all
                    // files in a directory whose names start with '_' (e.g. _shp_fix_782054.shp).
                    // Fall back to a raw Hadoop FS listing on an executor (which carries the UC
                    // Volume credential and has no '_'-prefix filter) and strip only known
                    // Spark/Hadoop job markers.
                    val hc = new SerializableConfiguration(spark.sessionState.newHadoopConf)
                    val path = new Path(new URI(cp))
                    val isDir = try {
                        val fs = path.getFileSystem(hc.value)
                        fs.isDirectory(path)
                    } catch { case _: Throwable => false }
                    if (isDir) {
                        val listed = try listDirOnExecutor(spark, cp) catch { case _: Throwable => Seq.empty }
                        val filtered = listed.filterNot(p => isMarkerFile(new Path(new URI(p))))
                        if (filtered.nonEmpty) filtered else Seq(cp)
                    } else {
                        Seq(cp)
                    }
                }
            } catch {
                case _: Throwable =>
                    val hc = new SerializableConfiguration(spark.sessionState.newHadoopConf)
                    try listHadoopFiles(cp, hc) catch { case _: Throwable => Seq(cp) }
            }
        }
    }

    /** Lists files in a directory using Hadoop FileSystem on an executor, bypassing Spark's hidden-
      * file filter (which skips `_`-prefixed names unconditionally). Marker files that are genuine
      * Hadoop/Spark job artifacts are dropped; real data files with `_`-prefixed names are kept. */
    private def listDirOnExecutor(
        spark: org.apache.spark.sql.SparkSession,
        cp: String
    ): Seq[String] = {
        val hc = new SerializableConfiguration(spark.sessionState.newHadoopConf)
        spark.sparkContext.parallelize(Seq(cp), 1).flatMap { p =>
            val path = new Path(new URI(p))
            val fs = path.getFileSystem(hc.value)
            if (fs.isDirectory(path)) {
                fs.listStatus(path)
                    .filterNot(_.isDirectory)
                    .map(_.getPath.toString)
                    .toSeq
            } else {
                Seq(p)
            }
        }.collect().toSeq.sorted
    }

    /** Lists the parent directory of a bare `.shp` path via Spark's credential-aware binaryFile
      * reader, returning all same-stem sibling files (including the `.shp` itself). Used to discover
      * sidecar files (`.shx`, `.dbf`, `.prj`, etc.) when the caller supplied only the `.shp` path
      * and the `candidates` list therefore contains no sidecars. */
    private def discoverShpSiblingsSpark(
        spark: org.apache.spark.sql.SparkSession,
        shpPath: String
    ): Seq[String] = {
        def baseName(p: String): String = p.replace("\\", "/").reverse.takeWhile(_ != '/').reverse
        val headName = baseName(shpPath)
        val dot = headName.lastIndexOf('.')
        val stem = if (dot > 0) headName.substring(0, dot) else headName
        // Build the parent directory path by stripping the file name.
        val parentPath = {
            val norm = shpPath.replace("\\", "/")
            val slash = norm.lastIndexOf('/')
            if (slash > 0) norm.substring(0, slash) else norm
        }
        try {
            val all = listDataFilesSpark(spark, parentPath)
            val siblings = all.filter { p =>
                val n = baseName(p)
                n == headName || n.startsWith(stem + ".")
            }
            if (siblings.nonEmpty) siblings else Seq(shpPath)
        } catch {
            case _: Throwable => Seq(shpPath)
        }
    }

    /** One way to read a file's bytes for OGR schema staging; `name` appears in the error when every
      * step fails. */
    private[util] final case class SchemaReadStep(name: String, read: String => Array[Byte])

    /** Escapes Hadoop glob characters so a `binaryFile` load matches exactly one literal path. */
    private[util] def escapeGlob(path: String): String =
        path.flatMap(c => if ("[]{}*?\\".indexOf(c.toInt) >= 0) s"\\$c" else c.toString)

    /** Reads `path` with a `binaryFile` query. Query tasks carry the UC Volume credential even on DCS
      * dedicated clusters, where the JVM process is refused direct `/Volumes` reads. */
    def binaryFileBytes(spark: SparkSession, path: String): Array[Byte] = {
        val rows = spark.read.format("binaryFile").load(escapeGlob(path)).select("content").collect()
        require(rows.length == 1, s"binaryFile matched ${rows.length} files for $path")
        rows.head.getAs[Array[Byte]](0)
    }

    /** The default read order: java.nio on this thread; java.nio in a one-task Spark job (the executor
      * carries the credential on classic and Serverless); a `binaryFile` query (DCS dedicated). */
    private[util] def schemaReadSteps(spark: SparkSession): Seq[SchemaReadStep] = Seq(
      SchemaReadStep("driver java.nio", (lp: String) => java.nio.file.Files.readAllBytes(new java.io.File(lp).toPath)),
      SchemaReadStep(
        "executor java.nio",
        (lp: String) =>
            spark.sparkContext
                .parallelize(Seq(lp), 1)
                .map(p => java.nio.file.Files.readAllBytes(new java.io.File(p).toPath))
                .collect()
                .head
      ),
      SchemaReadStep("binaryFile query", (lp: String) => binaryFileBytes(spark, lp))
    )

    /** Returns the bytes from the first step that succeeds; if every step fails, throws an
      * IOException listing each step's error. */
    private[util] def readBytesForSchema(path: String, steps: Seq[SchemaReadStep]): Array[Byte] =
        steps.foldLeft[Either[List[String], Array[Byte]]](Left(Nil)) {
            case (done @ Right(_), _) => done
            case (Left(errors), step) =>
                try Right(step.read(path))
                catch { case NonFatal(e) => Left(errors :+ s"${step.name}: ${e.getClass.getSimpleName}: ${e.getMessage}") }
        } match {
            case Right(bytes) => bytes
            case Left(errors) => throw new java.io.IOException(s"Could not read $path for schema inference (${errors.mkString("; ")})")
        }

    /** Stages a dataset's head file (plus any same-stem sidecars, e.g. shapefile `.shx`/`.dbf`/
      * `.prj`) to a local temp dir for OGR schema inference, returning the local head path. Reads
      * bytes through `readBytesForSchema`: java.nio on the driver, then in an executor task, then a
      * `binaryFile` query (DCS dedicated, where the JVM process is refused direct `/Volumes` reads).
      * `candidates` is the `listDataFilesSpark` listing, used to find the sidecars.
      *
      * When `headPath` is a bare `.shp` file path and `candidates` contains no sidecar siblings
      * (i.e. the caller listed only the single `.shp`), the parent directory is enumerated via
      * Spark's credential-aware file index so the full sidecar bundle (`.shx`, `.dbf`, `.prj`,
      * etc.) is staged alongside — without these GDAL throws "Unable to open .shx". */
    def stageHeadForSchemaSpark(
        spark: SparkSession,
        headPath: String,
        candidates: Seq[String]
    ): String = stageHeadForSchemaSpark(spark, headPath, candidates, schemaReadSteps(spark))

    private[util] def stageHeadForSchemaSpark(
        spark: SparkSession,
        headPath: String,
        candidates: Seq[String],
        steps: Seq[SchemaReadStep]
    ): String = {
        def baseName(p: String): String = p.replace("\\", "/").reverse.takeWhile(_ != '/').reverse
        def toPosix(p: String): String = p.stripPrefix("file:").stripPrefix("dbfs:")
        val headName = baseName(headPath)
        val dot = headName.lastIndexOf('.')
        val stem = if (dot > 0) headName.substring(0, dot) else headName
        val fromCandidates = candidates.filter { p =>
            val n = baseName(p)
            n == headName || n.startsWith(stem + ".")
        }.distinct
        // When the head is a bare .shp and candidates has no sidecars, discover them from the
        // parent directory so GDAL can open the full shapefile bundle.
        val siblings =
            if (fromCandidates.size <= 1 && headName.toLowerCase(java.util.Locale.ROOT).endsWith(".shp"))
                discoverShpSiblingsSpark(spark, headPath).distinct
            else
                fromCandidates
        val toStage = if (siblings.isEmpty) Seq(headPath) else siblings
        val tmpDir = java.nio.file.Files.createTempDirectory("gbx_ogr_schema_").toFile
        for (p <- toStage) {
            val local = toPosix(p)
            // Defensive guard: never attempt to read a directory as bytes. This can happen if
            // listDataFilesSpark falls back to the directory path itself (e.g. all files were
            // filtered out). Skip with a clear error rather than the cryptic "Is a directory".
            if (new java.io.File(local).isDirectory) {
                throw new IllegalArgumentException(
                    s"stageHeadForSchemaSpark: expected a file but got a directory: $local. " +
                    s"The directory may contain no readable data files."
                )
            }
            val bytes = readBytesForSchema(local, steps)
            java.nio.file.Files.write(new java.io.File(tmpDir, baseName(p)).toPath, bytes)
        }
        new java.io.File(tmpDir, headName).getAbsolutePath
    }

    /** Stages a FileGDB / zipped OGR dataset for schema inference through credentialed reads, for when
      * NodeFileManager's analyzer-thread Hadoop copy is refused (DCS dedicated). A file (`.zip`,
      * `.gdb.zip`) goes through `readBytesForSchema`; a `.gdb` directory is read with one `binaryFile`
      * query over its (flat) files. Returns the local path to open. */
    def stageDatasetForSchemaSpark(spark: SparkSession, datasetPath: String): String =
        stageDatasetForSchemaSpark(spark, datasetPath, schemaReadSteps(spark))

    private[util] def stageDatasetForSchemaSpark(
        spark: SparkSession,
        datasetPath: String,
        steps: Seq[SchemaReadStep]
    ): String = {
        def baseName(p: String): String = p.replace("\\", "/").stripSuffix("/").reverse.takeWhile(_ != '/').reverse
        val local = datasetPath.stripPrefix("file:").stripPrefix("dbfs:").stripSuffix("/")
        val name = baseName(local)
        val tmpDir = java.nio.file.Files.createTempDirectory("gbx_ogr_schema_").toFile
        if (name.toLowerCase(java.util.Locale.ROOT).endsWith(".gdb")) {
            val outDir = new java.io.File(tmpDir, name)
            outDir.mkdirs()
            val rows = spark.read.format("binaryFile").load(escapeGlob(local)).select("path", "content").collect()
            require(rows.nonEmpty, s"No files found in FileGDB directory $datasetPath")
            rows.foreach(r =>
                java.nio.file.Files.write(new java.io.File(outDir, baseName(r.getString(0))).toPath, r.getAs[Array[Byte]](1))
            )
            outDir.getAbsolutePath
        } else {
            val out = new java.io.File(tmpDir, name)
            java.nio.file.Files.write(out.toPath, readBytesForSchema(local, steps))
            out.getAbsolutePath
        }
    }

    /** The stage-or-fall-back decision for FileGDB / zip schema inference: take NodeFileManager's copy
      * when `readRemote` succeeds and the file exists (`releaseAfter = true`, the caller releases it
      * after use); otherwise fall back to `fallback` (credentialed query reads, `releaseAfter = false`).
      * The `readRemote` failure is logged at warn and, if the fallback also throws, attached to the
      * fallback's exception as suppressed, so a genuine copy failure (permissions, corrupt file) is not
      * reduced to "try the other path". A copy that returned a missing path is released first. */
    private[gbx] def stageDatasetOrFallBack(
        path: String,
        readRemote: String => String,
        release: String => Unit,
        fallback: String => String
    ): (String, Boolean) = {
        val copied: Either[Throwable, String] =
            try Right(readRemote(path))
            catch { case NonFatal(e) => Left(e) }
        copied match {
            case Right(local) if new java.io.File(local).exists => (local, true)
            case _ =>
                val original: Throwable = copied.fold(
                  identity,
                  local => new java.io.FileNotFoundException(s"Node file copy of $path reported $local, which does not exist")
                )
                copied.foreach(_ => release(path))
                logWarning(s"Staging $path through the node file cache failed; falling back to query reads", original)
                try {
                    (fallback(path), false)
                } catch {
                    case NonFatal(e) =>
                        if (e ne original) e.addSuppressed(original)
                        throw e
                }
        }
    }

    /** Returns the distinct `.shp` stems (base names without extension) present in a file listing.
      * Handles both plain `.shp` files and `.shp.zip` archives (e.g. `roads.shp.zip` → stem `roads`).
      * Used to detect multi-stem shapefile directories that may have divergent schemas. */
    def shpStems(files: Seq[String]): Seq[String] =
        files
            .flatMap { p =>
                val n = p.replace("\\", "/").reverse.takeWhile(_ != '/').reverse
                val lower = n.toLowerCase(java.util.Locale.ROOT)
                if (lower.endsWith(".shp.zip")) {
                    // Strip the double suffix .shp.zip to get the stem.
                    Some(n.substring(0, n.length - ".shp.zip".length))
                } else if (lower.endsWith(".shp")) {
                    val dot = n.lastIndexOf('.')
                    if (dot > 0) Some(n.substring(0, dot)) else Some(n)
                } else {
                    None
                }
            }
            .distinct

    /** Lists immediate subdirectories under inPath (non-recursive). */
    def listHadoopDirs(inPath: String, hconf: SerializableConfiguration): Seq[String] = {
        val path = new Path(new URI(cleanPath(inPath)))
        withFileSystem(path, hconf) { fs =>
            if (!fs.exists(path)) Seq.empty[String]
            else fs
                .listStatus(path)
                .filter(_.isDirectory)
                .map(_.getPath.toString)
                .toSeq
        }
    }

    /** Recursively lists files under inPath, optionally filtered by regex and excluding empty files. */
    def listAllHadoopFiles(
        inPath: String,
        hconf: SerializableConfiguration,
        regexFilter: String,
        dropEmpty: Boolean = false
    ): mutable.Seq[String] = {
        val filter = if (regexFilter == "") ".*" else s".*$regexFilter.*"
        val path = new Path(new URI(cleanPath(inPath)))
        withFileSystem(path, hconf) { fs =>
            val it = fs.listFiles(path, true) // recursive
            val files = scala.collection.mutable.ArrayBuffer[String]()
            while (it.hasNext) {
                val fileStatus = it.next()
                if (!dropEmpty || fileStatus.getLen > 0) {
                    if (regexFilter == "" || filter == ".*") {
                        files += fileStatus.getPath.toString
                    } else if (fileStatus.getPath.toString.matches(filter)) {
                        files += fileStatus.getPath.toString
                    }
                }
            }
            files
        }
    }

    /** Copies a file or directory from inPath to outPath; returns path to copied item in outDir. */
    // RAKE GUARD: this reads /Volumes on the EXECUTOR via Hadoop FS on the BARE /Volumes path
    // (see cleanPath) -> the UC-integrated connector, which IS credentialed in DSV2 tasks. Do NOT
    // "fix" a /Volumes read failure here by rewriting to file: or by adding a POSIX java.io copy:
    // both force the raw FUSE mount, which is uncredentialed in opaque DSV2 executor tasks and
    // fails with "Operation not permitted" / FileNotFound. (Tried in 0.4.0; reverted. The real
    // fix was keeping /Volumes bare in cleanPath; validated against released 0.3.0.)
    def copyToPath(
        inPath: String,
        outPath: String,
        hconf: SerializableConfiguration
    ): String = {
        val copyFromPath = new Path(cleanPath(inPath))
        val outputDir = withFileSystem(copyFromPath, hconf) { srcFS =>
            if (srcFS.getFileStatus(copyFromPath).isDirectory) {
                new Path(cleanPath(outPath)).toString
            } else {
                new Path(cleanPath(outPath)).getParent.toString
            }
        }
        copyToLocalDir(copyFromPath.toString, outputDir, hconf)
    }

    /** Copies files from srcFs whose names start with baseSrcPath prefix into dstDirPath on dstFs. */
    def copyRelativeFiles(
        srcFs: FileSystem,
        dstFs: FileSystem,
        baseSrcPath: Path,
        dstDirPath: Path
    ): Unit = {
        val extension = baseSrcPath.getName.split("\\.").lastOption.getOrElse("")
        val baseName = baseSrcPath.getName.stripSuffix(s".$extension")
        val prefix = baseName + "."

        val filter = new PathFilter {
            override def accept(path: Path): Boolean = path.getName.startsWith(prefix)
        }

        val parentDir = baseSrcPath.getParent
        val matchingFiles = srcFs.listStatus(parentDir, filter)

        matchingFiles.foreach { fileStatus =>
            val srcFile = fileStatus.getPath
            val dstFile = new Path(dstDirPath, srcFile.getName)
            AtomicDistributedCopy.copyIfNeeded(srcFs, dstFs, srcFile, dstFile)
        }
    }

    /** Copies inPath (file or dir) into outDir; for multi-file sources (e.g. .shp) copies all related files. Returns path to result. */
    def copyToLocalDir(inPath: String, outDir: String, hConf: SerializableConfiguration): String = {
        val copyFromPath = new Path(cleanPath(inPath))
        val outDirPath = new Path(cleanPath(outDir))
        withFileSystem(copyFromPath, hConf) { srcFS =>
            withFileSystem(outDirPath, hConf) { dstFS =>
                if (!dstFS.exists(outDirPath)) dstFS.mkdirs(outDirPath)

                if (srcFS.getFileStatus(copyFromPath).isDirectory) {
                    val dst = new Path(outDirPath, copyFromPath.getName)
                    AtomicDistributedCopy.copyIfNeeded(srcFS, dstFS, copyFromPath, dst)
                    dst.toString
                } else {
                    if (!dstFS.exists(outDirPath)) dstFS.mkdirs(outDirPath)
                    copyRelativeFiles(srcFS, dstFS, copyFromPath, outDirPath)
                    val fileName = copyFromPath.getName
                    s"$outDirPath/$fileName"
                }
            }
        }
    }

    /** Reads file at status.getPath into a byte array; caller closes stream via try/finally. */
    def readContent(fs: FileSystem, status: FileStatus): Array[Byte] = {
        val stream = fs.open(status.getPath)
        try { // noinspection UnstableApiUsage
            ByteStreams.toByteArray(stream)
        } finally { // noinspection UnstableApiUsage
            Closeables.close(stream, true)
        }
    }

    /** Runs `body` with the Hadoop FileSystem for `path` (resolved from the caller's config).
      *
      * NOTE (issue #34): this reads local / DBFS / Workspace paths. It does NOT read UC Volume
      * (`/Volumes/...`) paths from the JVM in the Spark execution context — the UC FUSE credential
      * is held only by Spark's managed Python worker, so `/Volumes` reads go through the Python
      * implementation (`gbx_rst_fromfile` is registered as the `pyrx` UDF; or use `binaryFile` +
      * `gbx_rst_fromcontent`). The Scala readers/`rst_fromfile` here serve non-Volume paths. */
    private def withFileSystem[T](path: Path, hconf: SerializableConfiguration)(body: FileSystem => T): T = {
        body(path.getFileSystem(hconf.value))
    }

    /** Reads the bytes at `rawPath` through the Hadoop FileSystem (local / DBFS / Workspace).
      * Not for UC Volume (`/Volumes/...`) paths from the JVM — see `withFileSystem` (issue #34). */
    def readBytes(rawPath: String, hConf: SerializableConfiguration): Array[Byte] = {
        val p = new Path(cleanPath(rawPath))
        withFileSystem(p, hConf) { fs => readContent(fs, fs.getFileStatus(p)) }
    }

    /** Deletes the path recursively if it exists. */
    def deleteIfExists(tmpPath: String, hconf: SerializableConfiguration): Unit = {
        val cleanPath = HadoopUtils.cleanPath(tmpPath)
        val path = new Path(cleanPath)
        withFileSystem(path, hconf) { fs =>
            if (fs.exists(path)) {
                fs.delete(path, true)
            }
        }
    }

    /** Returns total size in bytes (file length or directory content summary). */
    def getSize(path: String, hConf: SerializableConfiguration): Long = {
        val cleanPath = new Path(HadoopUtils.cleanPath(path))
        withFileSystem(cleanPath, hConf) { fs =>
            val status = fs.getFileStatus(cleanPath)
            if (status.isDirectory) {
                fs.getContentSummary(cleanPath).getLength
            } else {
                status.getLen
            }
        }
    }

    /** Recognized geo extensions, longest-first so multi-part suffixes match before their parts.
      * Mirrors the light-tier `_RECOGNIZED_EXTS` tuple in `ds/vector.py`. */
    private val RECOGNIZED_GEO_EXTS: Seq[String] =
        Seq(".shp.zip", ".gdb.zip", ".pmtiles", ".gpkg", ".geojson", ".gdb", ".shp")

    /** Returns `name` with `ext` appended if it doesn't already end with `ext` (case-insensitive).
      *
      * - If `name` already ends with `ext` → return unchanged.
      * - If `name` ends with a DIFFERENT recognized geo extension → throw a clear error.
      * - Otherwise → append `ext`.
      *
      * For multi-part `ext` values (e.g. `.shp.zip`) the incremental-completion case from the
      * Python implementation is not needed here because the only ext in scope for heavy PMTiles
      * is `.pmtiles` (a single-part extension). */
    private[gbx] def completeExt(name: String, ext: String): String = {
        val low = name.toLowerCase(java.util.Locale.ROOT)
        if (low.endsWith(ext)) name
        else {
            val conflict = RECOGNIZED_GEO_EXTS.find(other => other != ext && low.endsWith(other))
            conflict match {
                case Some(other) =>
                    throw new IllegalArgumentException(
                        s"output name '$name' ends with '$other' but this writer " +
                        s"expected $ext (got a different geo extension)."
                    )
                case None => name + ext
            }
        }
    }

    /** Resolves the output path for a single-file writer, mirroring the Python 3-case contract
      * in `_resolve_single_file_output` (ds/vector.py).
      *
      * Case 1 — `fileName` is Some(name): treat `path` as the parent directory (create it +
      *   parents); return `path/complete(fileName, ext)`.
      * Case 2 — `fileName` is None and `path` is an existing directory: return
      *   `path/complete(basename(path), ext)`.
      * Case 3 — `fileName` is None and `path` is file-like / doesn't exist yet: return
      *   `complete(path, ext)`; create parent directory.
      *
      * Filesystem checks use Hadoop FS on the `cleanPath`'d form — `/Volumes` paths are kept
      * BARE (UC connector) as required by `cleanPath`; never `file:` for `/Volumes`. */
    def resolveSingleFileOutput(
        path: String,
        fileName: Option[String],
        ext: String,
        hConf: SerializableConfiguration
    ): String = {
        val stripped = path.stripSuffix("/")
        val cleanedPath = new org.apache.hadoop.fs.Path(new java.net.URI(cleanPath(stripped)))

        fileName match {
            case Some(name) if name.nonEmpty =>
                // Case 1: path is the parent dir; create it + return path/complete(name, ext).
                withFileSystem(cleanedPath, hConf) { fs => fs.mkdirs(cleanedPath) }
                s"$stripped/${completeExt(name, ext)}"

            case _ =>
                // Check if path is an existing directory.
                val isDir = try {
                    withFileSystem(cleanedPath, hConf) { fs =>
                        fs.exists(cleanedPath) && fs.getFileStatus(cleanedPath).isDirectory
                    }
                } catch { case _: Throwable => false }

                if (isDir) {
                    // Case 2: existing dir -> name output after the dir, place it under it.
                    val baseName = new org.apache.hadoop.fs.Path(stripped).getName
                    s"$stripped/${completeExt(baseName, ext)}"
                } else {
                    // Case 3: file-like target -> complete ext, create parent dir.
                    val parentPath = new org.apache.hadoop.fs.Path(cleanedPath.getParent.toString)
                    withFileSystem(parentPath, hConf) { fs => fs.mkdirs(parentPath) }
                    completeExt(stripped, ext)
                }
        }
    }

    /** Murmur3 hash of path + length + modification time; used as stable file id. */
    def getUUID(status: FileStatus): Long = {
        val uuid = Murmur3.hash64(
          status.getPath.toString.getBytes("UTF-8") ++
              status.getLen.toString.getBytes("UTF-8") ++
              status.getModificationTime.toString.getBytes("UTF-8")
        )
        uuid
    }

}
