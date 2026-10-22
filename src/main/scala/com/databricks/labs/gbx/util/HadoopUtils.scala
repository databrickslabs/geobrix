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

    // Metadata-only names: files that sit next to data (ArcGIS `map.shp.xml`, GDAL `*.aux.xml`, a README) and
    // are not datasets. Matched by suffix on the whole name, since `fileExt` sees only the last extension.
    // Bare `.xml` is NOT listed: GML and NAS files are `.xml` datasets.
    private val OGR_METADATA_SUFFIXES: Seq[String] = Seq(".shp.xml", ".aux.xml", ".txt", ".md")

    /** True for a file that only describes data and is never a dataset to read: a name ending `.shp.xml`,
      * `.aux.xml`, `.txt` or `.md` (any case). Planning and schema-head selection skip these. */
    def isMetadataOnly(p: String): Boolean = {
        val n = p.toLowerCase(java.util.Locale.ROOT)
        OGR_METADATA_SUFFIXES.exists(n.endsWith)
    }

    /** Picks the PRIMARY data file from a listing for schema inference — the file the OGR driver
      * actually opens, NOT a sidecar. A shapefile directory lists (sorted) as
      * `[.cpg, .dbf, .prj, .shp, .shx]`; `files.head` would be `.cpg`, which OGR cannot open as an
      * ESRI Shapefile (it threw `Could not open dataset ... at .../<name>.cpg`). Excluding the known
      * shapefile sidecars makes `.shp` the head; non-shapefile datasets (`.geojson`, `.gpkg`, ...)
      * have no matching sidecars and are unaffected. Metadata-only files (`isMetadataOnly`) are never the head
      * either: a `README.txt` sorts first and is not a dataset. `stageHeadForSchemaSpark` still stages the
      * stem-siblings alongside, so the driver sees the full bundle. */
    def primaryDataFile(files: Seq[String]): Option[String] =
        files.find(p => !OGR_SIDECAR_EXTS.contains(fileExt(p)) && !isMetadataOnly(p)).orElse(files.headOption)

    /** Filters a file listing to primary data files, excluding OGR shapefile sidecar extensions
      * (`.dbf`, `.shx`, `.prj`, `.cpg`, etc.) that the ESRI Shapefile driver opens as
      * attribute-only datasources. All non-sidecar files (`.shp`, `.zip`, `.geojson`, `.gpkg`,
      * etc.) are kept — this supports multi-stem directories and heterogeneous driver mixes.
      * Used in the OGR batch read path to prevent loose sidecars from being double-counted. Metadata-only
      * files (`isMetadataOnly`) are excluded too, as they are not datasets. */
    def primaryDataFiles(files: Seq[String]): Seq[String] =
        files.filterNot(p => OGR_SIDECAR_EXTS.contains(fileExt(p)) || isMetadataOnly(p))

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
      * and has no `_`-prefix filter — stripping only known Spark/Hadoop job-marker files. The paths
      * come back as plain paths (`file:///tmp/my file.tif`), not as Spark's URL-encoded file names
      * (`my%20file.tif`), which no Hadoop `Path(String)` or file copy resolves. */
    def listDataFilesSpark(spark: org.apache.spark.sql.SparkSession, inPath: String): Seq[String] =
        listDataFilesChecked(spark, inPath)._1

    /** `listDataFilesSpark`, and whether `inPath` is a directory that holds no data files. For such a
      * directory the listing is `Seq(inPath)` (the same fall-back as a path that cannot be listed), so a
      * caller that reads rather than infers can treat it as zero files. */
    private[gbx] def listDataFilesChecked(
        spark: org.apache.spark.sql.SparkSession,
        inPath: String
    ): (Seq[String], Boolean) = {
        val cp = cleanPath(inPath)
        val lower = inPath.toLowerCase(java.util.Locale.ROOT).stripSuffix("/")
        if (lower.endsWith(".gdb") || lower.endsWith(".gdb.zip") || lower.endsWith(".zip")) {
            (Seq(cp), false)
        } else {
            try {
                val files = spark.read.format("binaryFile").load(cp).inputFiles.toSeq.map(decodeInputFile).sorted
                if (files.nonEmpty) {
                    (files, false)
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
                        if (filtered.nonEmpty) (filtered, false) else (Seq(cp), true)
                    } else {
                        (Seq(cp), false)
                    }
                }
            } catch {
                case _: Throwable =>
                    val hc = new SerializableConfiguration(spark.sessionState.newHadoopConf)
                    (try listHadoopFiles(cp, hc) catch { case _: Throwable => Seq(cp) }, false)
            }
        }
    }

    /** `p` with the percent-escapes of its path decoded: Spark's `inputFiles` are URL-encoded
      * (`file:///d/my%20file%231.tif`). The scheme, authority and the rest stay as they are, and a name
      * that is not a valid URI (it is already plain, a literal `%` or a space) is returned unchanged. */
    private[util] def decodeInputFile(p: String): String =
        try {
            val uri = new URI(p)
            val raw = uri.getRawPath
            if (raw == null || !raw.contains('%')) p
            else {
                val i = p.lastIndexOf(raw)
                p.substring(0, i) + uri.getPath + p.substring(i + raw.length)
            }
        } catch {
            case _: java.net.URISyntaxException => p
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
      * step fails. `skipsAfterRefusal` marks a step that goes through the same JVM file access as an
      * earlier step: once any step failed with "Operation not permitted" (see `isFileAccessRefused`)
      * it is refused the same way, so it is skipped rather than run as a failed Spark job. */
    private[util] final case class SchemaReadStep(
        name: String,
        read: String => Array[Byte],
        skipsAfterRefusal: Boolean = false
    )

    /** True when `e` (or its cause) is the refusal a DCS dedicated cluster gives the JVM process for
      * `/Volumes` files: a `java.nio.file.AccessDeniedException`, or a `FileSystemException` whose
      * reason or message says "Operation not permitted". */
    private[util] def isFileAccessRefused(e: Throwable): Boolean = {
        def refused(t: Throwable): Boolean = t match {
            case _: java.nio.file.AccessDeniedException => true
            case f: java.nio.file.FileSystemException =>
                Seq(f.getReason, f.getMessage).exists(m => m != null && m.contains("Operation not permitted"))
            case _ => false
        }
        refused(e) || (e.getCause != null && (e.getCause ne e) && refused(e.getCause))
    }

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
      * carries the credential on classic and Serverless); a `binaryFile` query (DCS dedicated). The
      * executor java.nio step is skipped once the driver one was refused: the JVM is refused the same
      * way in an executor task on DCS, and each attempt would be a failed Spark job. */
    private[util] def schemaReadSteps(spark: SparkSession): Seq[SchemaReadStep] = Seq(
      SchemaReadStep("driver java.nio", (lp: String) => java.nio.file.Files.readAllBytes(new java.io.File(lp).toPath)),
      SchemaReadStep(
        "executor java.nio",
        (lp: String) =>
            spark.sparkContext
                .parallelize(Seq(lp), 1)
                .map(p => java.nio.file.Files.readAllBytes(new java.io.File(p).toPath))
                .collect()
                .head,
        skipsAfterRefusal = true
      ),
      SchemaReadStep("binaryFile query", (lp: String) => binaryFileBytes(spark, lp))
    )

    /** Reads the files of ONE staging call (a head file plus its sidecars) through `steps`. A step
      * that failed is not retried for the next file: reading starts from the step that last
      * succeeded (steps are ordered by preference, so the earlier ones are the ones that failed).
      * Once a step fails with "Operation not permitted", steps marked `skipsAfterRefusal` are
      * skipped for the rest of the call. */
    private[util] final class SchemaReader(steps: Seq[SchemaReadStep]) {
        private var first = 0
        private var refused = false

        /** Returns the bytes from the first usable step; if every step fails, throws an IOException
          * listing each step's outcome. */
        def read(path: String): Array[Byte] = {
            @scala.annotation.tailrec
            def attempt(i: Int, errors: Vector[String]): Array[Byte] =
                if (i >= steps.length) {
                    throw new java.io.IOException(s"Could not read $path for schema inference (${errors.mkString("; ")})")
                } else {
                    val step = steps(i)
                    if (refused && step.skipsAfterRefusal) {
                        attempt(i + 1, errors :+ s"${step.name}: skipped, the JVM is refused direct file reads")
                    } else {
                        val result =
                            try Right(step.read(path))
                            catch { case NonFatal(e) => Left(e) }
                        result match {
                            case Right(bytes) =>
                                first = i
                                bytes
                            case Left(e) =>
                                if (isFileAccessRefused(e)) refused = true
                                attempt(i + 1, errors :+ s"${step.name}: ${e.getClass.getSimpleName}: ${e.getMessage}")
                        }
                    }
                }
            attempt(first, Vector.empty)
        }
    }

    /** Returns the bytes from the first step that succeeds; if every step fails, throws an
      * IOException listing each step's error. One call, one file: see `SchemaReader` for several. */
    private[util] def readBytesForSchema(path: String, steps: Seq[SchemaReadStep]): Array[Byte] =
        new SchemaReader(steps).read(path)

    /** Stages a dataset's head file (plus any same-stem sidecars, e.g. shapefile `.shx`/`.dbf`/
      * `.prj`) to a local temp dir for OGR schema inference, returning the local head path. Reads
      * bytes through one `SchemaReader`: java.nio on the driver, then in an executor task, then a
      * `binaryFile` query (DCS dedicated, where the JVM process is refused direct `/Volumes` reads).
      * The sidecars start from the step that read the head, so a refused step is not retried per file.
      * `candidates` is the `listDataFilesSpark` listing, used to find the sidecars.
      *
      * When `headPath` is a bare `.shp` file path and `candidates` contains no sidecar siblings
      * (i.e. the caller listed only the single `.shp`), the parent directory is enumerated via
      * Spark's credential-aware file index so the full sidecar bundle (`.shx`, `.dbf`, `.prj`,
      * etc.) is staged alongside - without these GDAL throws "Unable to open .shx".
      *
      * The copy lives in a `gbx_ogr_schema_*` temp dir that the caller removes with `discardSchemaCopy`
      * once it has inferred from it; a staging that throws removes its own dir. */
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
        withSchemaStagingDir { tmpDir =>
            val reader = new SchemaReader(steps)
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
                val bytes = reader.read(local)
                java.nio.file.Files.write(new java.io.File(tmpDir, baseName(p)).toPath, bytes)
            }
            new java.io.File(tmpDir, headName).getAbsolutePath
        }
    }

    /** Bytes one schema read can hold: a file is read into one byte array, and a `binaryFile` query's
      * `content` is capped at this (Spark's `ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH`), just under 2 GiB. */
    private[util] val MaxSchemaReadBytes: Long = Int.MaxValue - 15L

    /** Name prefix of the temp dirs schema staging writes copies into (see `discardSchemaCopy`). */
    private val SchemaStagingPrefix = "gbx_ogr_schema_"

    /** Makes a schema-staging temp dir and runs `body` in it. The dir is the caller's to remove with
      * `discardSchemaCopy` once the staged copy has been used, except that it is deleted here when `body`
      * throws, since the caller never receives a path to discard. */
    private def withSchemaStagingDir(body: java.io.File => String): String = {
        val tmpDir = java.nio.file.Files.createTempDirectory(SchemaStagingPrefix).toFile
        var staged = false
        try {
            val local = body(tmpDir)
            staged = true
            local
        } finally {
            if (!staged) NodeFilePathUtil.deleteRecursively(tmpDir.toPath)
        }
    }

    /** The length of `local`: from the file system when this JVM can stat it, else from a `binaryFile`
      * listing (metadata only, the content is not read); None when neither tells. */
    private def schemaReadLength(spark: SparkSession, local: String): Option[Long] = {
        val f = new java.io.File(local)
        if (f.isFile) {
            Some(f.length())
        } else {
            scala.util
                .Try(spark.read.format("binaryFile").load(escapeGlob(local)).select("length").collect())
                .toOption
                .flatMap(_.headOption.map(_.getLong(0)))
        }
    }

    /** Stages a FileGDB / zipped OGR dataset for schema inference through credentialed reads, for when
      * NodeFileManager's analyzer-thread Hadoop copy is refused (DCS dedicated). A file (`.zip`,
      * `.gdb.zip`) goes through `readBytesForSchema`, and one over `MaxSchemaReadBytes` is refused with
      * an error naming the limit. A `.gdb` directory is read with one `binaryFile` query over its (flat)
      * files, fetched to the driver a partition at a time and written as each arrives, so the driver
      * never holds the whole dataset. Returns the local path to open; the caller removes its temp dir with
      * `discardSchemaCopy`, and a staging that throws removes its own. */
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
        val isGdbDir = name.toLowerCase(java.util.Locale.ROOT).endsWith(".gdb")
        if (!isGdbDir) schemaReadLength(spark, local).filter(_ > MaxSchemaReadBytes).foreach { length =>
            throw new java.io.IOException(
              s"Cannot read $datasetPath for schema inference: it is $length bytes, and a schema read holds " +
                  s"one file of at most $MaxSchemaReadBytes bytes (2 GiB, the binaryFile content limit). " +
                  "Unzip it and read the unzipped dataset (a .gdb folder is read file by file)."
            )
        }
        withSchemaStagingDir { tmpDir =>
            if (isGdbDir) {
                val outDir = new java.io.File(tmpDir, name)
                outDir.mkdirs()
                val rows = spark.read.format("binaryFile").load(escapeGlob(local)).select("path", "content").toLocalIterator()
                var written = 0
                while (rows.hasNext) {
                    val r = rows.next()
                    java.nio.file.Files.write(new java.io.File(outDir, baseName(r.getString(0))).toPath, r.getAs[Array[Byte]](1))
                    written += 1
                }
                require(written > 0, s"No files found in FileGDB directory $datasetPath")
                outDir.getAbsolutePath
            } else {
                val out = new java.io.File(tmpDir, name)
                java.nio.file.Files.write(out.toPath, readBytesForSchema(local, steps))
                out.getAbsolutePath
            }
        }
    }

    /** Deletes the temp copy `stageDatasetForSchemaSpark` (or `stageHeadForSchemaSpark`) made: the
      * `gbx_ogr_schema_*` temp dir holding `local`, recursively. A path in any other dir is left alone. */
    private[gbx] def discardSchemaCopy(local: String): Unit = {
        val dir = java.nio.file.Paths.get(local).toAbsolutePath.normalize.getParent
        val tmp = java.nio.file.Paths.get(System.getProperty("java.io.tmpdir")).toAbsolutePath.normalize
        if (dir != null && dir.getParent == tmp && dir.getFileName.toString.startsWith(SchemaStagingPrefix)) {
            NodeFilePathUtil.deleteRecursively(dir)
        }
    }

    /** True when a node-file copy can be opened: an existing file, or a directory with at least one
      * file directly in it (an empty `.gdb` folder, or one holding only a subfolder, is no FileGDB). */
    private def isUsableCopy(local: String): Boolean = {
        val f = new java.io.File(local)
        if (f.isDirectory) Option(f.listFiles()).exists(_.exists(_.isFile)) else f.exists
    }

    /** The stage-or-fall-back decision for FileGDB / zip schema inference: take NodeFileManager's copy
      * when `readRemote` succeeds and the copy is usable (`isUsableCopy`; `releaseAfter = true`, the
      * caller releases it after use); otherwise fall back to `fallback` (credentialed query reads,
      * `releaseAfter = false`). The `readRemote` failure is logged at warn and, if the fallback also
      * throws, attached to the fallback's exception as suppressed, so a genuine copy failure
      * (permissions, corrupt file) is not reduced to "try the other path". A copy that returned a
      * missing path or a directory with no files in it is released first. */
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
            case Right(local) if isUsableCopy(local) => (local, true)
            case _ =>
                val original: Throwable = copied.fold(
                  identity,
                  local =>
                      new java.io.FileNotFoundException(
                        if (new java.io.File(local).isDirectory)
                            s"Node file copy of $path reported $local, a directory with no files in it"
                        else s"Node file copy of $path reported $local, which does not exist"
                      )
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

    /** Copies a file or directory `inPath` into `outPath`'s parent under the source's own name (plus a
      * file's same-stem sidecars); returns the copied item's path. Callers name `outPath` after the
      * source (NodeFilePathUtil's `<cache>/<hash>/<name>`, the writers' `<root>/<name>`), so the copy
      * lands at `outPath`. A directory is handled like a file: copying it INTO `outPath` would produce
      * `outPath/<name>/...` and leave `outPath`, the path NodeFilePathUtil returns, empty. Whatever is
      * already at the destination is replaced (see [[AtomicDistributedCopy.copyOverwriting]]), so a writer
      * that writes the same output again leaves only the new one. */
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
        val outputDir = new Path(cleanPath(outPath)).getParent.toString
        copyToLocalDir(copyFromPath.toString, outputDir, hconf)
    }

    /** Copies files from srcFs whose names start with baseSrcPath prefix into dstDirPath on dstFs, replacing
      * any of the same name there. */
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
            AtomicDistributedCopy.copyOverwriting(srcFs, dstFs, srcFile, dstFile)
        }
    }

    /** Copies inPath (file or dir) into outDir, replacing what is there under its name; for multi-file sources
      * (e.g. .shp) copies all related files. Returns path to result. */
    def copyToLocalDir(inPath: String, outDir: String, hConf: SerializableConfiguration): String = {
        val copyFromPath = new Path(cleanPath(inPath))
        val outDirPath = new Path(cleanPath(outDir))
        withFileSystem(copyFromPath, hConf) { srcFS =>
            withFileSystem(outDirPath, hConf) { dstFS =>
                if (!dstFS.exists(outDirPath)) dstFS.mkdirs(outDirPath)

                if (srcFS.getFileStatus(copyFromPath).isDirectory) {
                    val dst = new Path(outDirPath, copyFromPath.getName)
                    AtomicDistributedCopy.copyOverwriting(srcFS, dstFS, copyFromPath, dst)
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
