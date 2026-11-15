package com.databricks.labs.gbx.util

import org.apache.hadoop.fs._
import org.apache.spark.internal.Logging
import org.apache.spark.util.SerializableConfiguration

/** Path normalization (Volumes, DBFS, file:) and copy for Hadoop filesystems. */
//noinspection ScalaWeakerAccess
object HadoopUtils extends Logging {

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

    /** GDAL sidecars of a file `<name>`: `<name>.aux.xml` (PAM metadata), `.ovr` (overviews), `.msk` (mask). */
    private val GdalSidecarSuffixes = Seq(".aux.xml", ".ovr", ".msk")

    /** Copies files from srcFs whose names start with baseSrcPath prefix into dstDirPath on dstFs, replacing
      * any of the same name there. When the file replaces an older one, the older one's GDAL sidecars that
      * the new file does not have are deleted first: GDAL would read an old `.aux.xml` as the new file's
      * metadata. */
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

        if (isFileAt(dstFs, new Path(dstDirPath, baseSrcPath.getName))) {
            val copied = matchingFiles.map(_.getPath.getName).toSet
            GdalSidecarSuffixes.map(baseSrcPath.getName + _).filterNot(copied).foreach { n =>
                val stale = new Path(dstDirPath, n)
                if (isFileAt(dstFs, stale) && !dstFs.delete(stale, false) && dstFs.exists(stale)) {
                    throw new java.io.IOException(s"Could not delete $stale, a sidecar of the older output being replaced")
                }
            }
        }

        matchingFiles.foreach { fileStatus =>
            val srcFile = fileStatus.getPath
            val dstFile = new Path(dstDirPath, srcFile.getName)
            AtomicDistributedCopy.copyOverwriting(srcFs, dstFs, srcFile, dstFile)
        }
    }

    /** Whether a file (not a directory) is at `p`. */
    private def isFileAt(fs: FileSystem, p: Path): Boolean =
        try fs.getFileStatus(p).isFile
        catch { case _: java.io.FileNotFoundException => false }

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

}
