package com.databricks.labs.gbx.util

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import java.time.{Duration, Instant}

/** Copy between Hadoop FileSystems with retry/wait until destination exists (for distributed consistency). */
object AtomicDistributedCopy {

    // Maximum wait time for file existence (10 seconds)
    private val MAX_WAIT_TIME_MS = 10000

    /** Copies srcPath to dstPath if dst does not exist; otherwise waits up to MAX_WAIT_TIME_MS for dst to appear. */
    def copyIfNeeded(
        srcFs: FileSystem,
        dstFs: FileSystem,
        srcPath: Path,
        dstPath: Path
    ): Unit = {
        if (!dstFs.exists(dstPath)) {
            try {
                val flag = FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, srcFs.getConf)
                if (!flag) {
                    throw new RuntimeException(s"Failed to copy $srcPath to $dstPath")
                }
            } catch {
                case _: Throwable => waitUntilFileExists(dstFs, dstPath)
            }
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
