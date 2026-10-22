package com.databricks.labs.gbx.util

import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}

import java.io.{FileNotFoundException, IOException}
import scala.util.control.NonFatal

/** Copies between Hadoop FileSystems, replacing whatever is at the destination. */
object AtomicDistributedCopy {

    /** Copies srcPath (a file or a directory) to dstPath, replacing what is there. A file is overwritten.
      * An existing directory, or anything at dstPath when a directory is copied, is deleted first, so a
      * directory output (Zarr) keeps no entry of the version it replaces and is never copied into it.
      *
      * Nothing is skipped: every read copy lands in a fresh generation dir of the node-file cache, so the
      * destination exists only when a writer writes the same output again, and keeping it (as was done
      * when it had the same length) kept stale output. Copying a path onto itself is a no-op.
      *
      * A failed copy throws an IOException carrying the cause: a partly written destination must not
      * pass for a finished copy. */
    def copyOverwriting(
        srcFs: FileSystem,
        dstFs: FileSystem,
        srcPath: Path,
        dstPath: Path
    ): Unit =
        if (srcFs.makeQualified(srcPath) != dstFs.makeQualified(dstPath)) {
            val copied =
                try {
                    val srcIsDir = srcFs.getFileStatus(srcPath).isDirectory
                    if (statusOf(dstFs, dstPath).exists(st => st.isDirectory || srcIsDir)) {
                        if (!dstFs.delete(dstPath, true) && dstFs.exists(dstPath)) {
                            throw new IOException(s"Could not delete the existing $dstPath")
                        }
                    }
                    FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, true, srcFs.getConf)
                } catch {
                    case NonFatal(e) => throw new IOException(s"Failed to copy $srcPath to $dstPath: ${e.getMessage}", e)
                }
            if (!copied) throw new IOException(s"Failed to copy $srcPath to $dstPath")
        }

    /** The status of `p`, or None when nothing is there. */
    private def statusOf(fs: FileSystem, p: Path): Option[FileStatus] =
        try Some(fs.getFileStatus(p))
        catch { case _: FileNotFoundException => None }

}
