package com.databricks.labs.gbx.util

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import java.io.IOException
import java.time.{Duration, Instant}
import scala.util.control.NonFatal

/** Copy between Hadoop FileSystems with retry/wait until destination exists (for distributed consistency). */
object AtomicDistributedCopy {

    // Maximum wait time for file existence (10 seconds)
    private val MAX_WAIT_TIME_MS = 10000

    /** Copies srcPath to dstPath when the destination is missing OR its length differs from
      * the source; otherwise waits up to MAX_WAIT_TIME_MS for dst to appear.
      *
      * The local cache path is keyed by the REMOTE PATH (not its content), so an
      * existence-only check would serve a STALE local copy when the remote file changed
      * under the same path (e.g. an overwrite within the same cluster session, on a
      * long-lived executor whose local_disk0 still holds the old copy). A length mismatch is
      * a cheap, reliable content-change signal for files; the copy then overwrites the stale
      * local file. (Directories report length 0, so the `.gdb`-directory path behaves as
      * before -- no regression.)
      *
      * A failed copy throws an IOException carrying the cause: a partly written destination
      * must not pass for a finished copy, so the caller deletes it and retries. */
    def copyIfNeeded(
        srcFs: FileSystem,
        dstFs: FileSystem,
        srcPath: Path,
        dstPath: Path
    ): Unit = {
        val stale =
            dstFs.exists(dstPath) &&
                dstFs.getFileStatus(dstPath).getLen != srcFs.getFileStatus(srcPath).getLen
        if (!dstFs.exists(dstPath) || stale) {
            val copied =
                try {
                    // overwrite=true so a stale local copy (wrong length) is replaced, not skipped.
                    FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, true, srcFs.getConf)
                } catch {
                    case NonFatal(e) => throw new IOException(s"Failed to copy $srcPath to $dstPath: ${e.getMessage}", e)
                }
            if (!copied) throw new IOException(s"Failed to copy $srcPath to $dstPath")
        } else {
            waitUntilFileExists(dstFs, dstPath)
        }
    }

    /** Polls until path exists or MAX_WAIT_TIME_MS elapses. */
    private def waitUntilFileExists(fs: FileSystem, path: Path): Unit = {
        val startTime = Instant.now()
        while (!fs.exists(path) && Duration.between(startTime, Instant.now()).toMillis < MAX_WAIT_TIME_MS) {
            Thread.sleep(200)
        }
    }

}
