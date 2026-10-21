package com.databricks.labs.gbx.util

import org.apache.spark.util.SerializableConfiguration

import java.util.concurrent.atomic.AtomicBoolean

/** Caches remote paths as node-local copies via NodeFilePathUtil; each read holds one reader on a
  * copy until it is released (by remote, by local path, or by closing a lease). */
object NodeFileManager {

    @volatile private var hconf: SerializableConfiguration = _

    /** Sets the Hadoop config used for copying; must be called before readRemote. */
    def init(hadoopConf: SerializableConfiguration): Unit = {
        hconf = hadoopConf
    }

    /** Copies remote path to the node cache (with sidecars) unless a current copy exists, holds one
      * reader on it, and returns its local path. */
    def readRemote(remotePath: String): String = {
        val conf = hconf
        require(conf != null, "NodeFileManager.init must be called before readRemote")
        NodeFilePathUtil.readLock(remotePath, conf)._1
    }

    /** Releases one reader of the CURRENT copy of `remotePath`; may delete it at its last release. A
      * copy's local path is not a remote: release a copy by its local path with `releaseLocal`. */
    def releaseRemote(remotePath: String): Unit = {
        NodeFilePathUtil.releaseByRemote(remotePath)
    }

    /** Releases one reader of the copy that owns `localPath`; an unknown path is a no-op. */
    def releaseLocal(localPath: String): Unit = {
        NodeFilePathUtil.releaseByLocal(localPath)
    }

    /** readRemote plus a lease whose close releases that one reader. */
    def acquire(remotePath: String): NodeFileLease = new NodeFileLease(readRemote(remotePath))

}

/** One reader held on a node-file copy; `close` releases it exactly once, however often called. */
final class NodeFileLease private[util] (val localPath: String) extends AutoCloseable {

    private val closed = new AtomicBoolean(false)

    override def close(): Unit = {
        if (closed.compareAndSet(false, true)) NodeFileManager.releaseLocal(localPath)
    }

}
