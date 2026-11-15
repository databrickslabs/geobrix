package com.databricks.labs.gbx.util

import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}

import java.io.{FileNotFoundException, IOException}
import scala.util.control.NonFatal

/** Copies between Hadoop FileSystems, replacing whatever is at the destination. */
object AtomicDistributedCopy {

    /** Copies srcPath (a file or a directory) to dstPath, replacing what is there. A file overwrites a file.
      * When a directory is copied, whatever is at dstPath is deleted first, so a directory output (Zarr)
      * keeps no entry of the version it replaces and is never copied into it. A file is never written over
      * a directory: that throws, and the directory is left as it is.
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
                    statusOf(dstFs, dstPath) match {
                        case Some(_) if srcIsDir =>
                            if (!dstFs.delete(dstPath, true) && dstFs.exists(dstPath)) {
                                throw new IOException(s"Could not delete the existing $dstPath")
                            }
                        case Some(st) if st.isDirectory => throw new FolderInTheWayException(srcPath, dstPath)
                        case _ => // nothing there, or a file the copy overwrites
                    }
                    FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, true, srcFs.getConf)
                } catch {
                    case e: FolderInTheWayException => throw e
                    case NonFatal(e)                => throw new IOException(s"Failed to copy $srcPath to $dstPath: ${e.getMessage}", e)
                }
            if (!copied) throw new IOException(s"Failed to copy $srcPath to $dstPath")
        }

    /** A file output whose destination is an existing folder: the folder is not the copy's to delete. */
    private final class FolderInTheWayException(src: Path, dst: Path)
        extends IOException(
          s"Cannot write the file $src to $dst: a folder of that name is in the way (it is left as it is; " +
              "remove or rename it, or write under another name)"
        )

    /** The status of `p`, or None when nothing is there. */
    private def statusOf(fs: FileSystem, p: Path): Option[FileStatus] =
        try Some(fs.getFileStatus(p))
        catch { case _: FileNotFoundException => None }

}
