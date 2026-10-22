package com.databricks.labs.gbx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.util.SerializableConfiguration

import java.util.concurrent.atomic.AtomicBoolean

/** Caches remote paths as node-local copies via NodeFilePathUtil; each read holds one reader on a
  * copy until it is released (by remote, by local path, or by closing a lease). A copy no reader holds
  * is kept for the retention window, so the next read of an unchanged remote reuses it. */
object NodeFileManager {

    /** Spark conf: how long, in milliseconds, a copy no read holds is kept for reuse; 0 deletes it at
      * once. Read from the ExpressionConfig that executor-side readers pass to `init`. */
    val RetentionKey = "spark.databricks.labs.gbx.nodecache.retention.ms"

    @volatile private var hconf: SerializableConfiguration = _

    /** Sets the Hadoop config used for copying; must be called before readRemote. Leaves the retention
      * as it is (the default, or what the last `init(conf)` in this JVM set). */
    def init(hadoopConf: SerializableConfiguration): Unit = {
        hconf = hadoopConf
    }

    /** Sets the Hadoop config used for copying and the retention from `conf`'s [[RetentionKey]]
      * (default 60000 ms; 0 deletes a copy at its last release). An invalid value is refused. */
    def init(conf: ExpressionConfig): Unit = {
        val ms = retentionOf(conf.configs)
        init(conf.hConf)
        NodeFilePathUtil.setRetentionMs(ms)
    }

    /** The retention [[RetentionKey]] asks for: a whole number of milliseconds, 0 or more. */
    private def retentionOf(configs: Map[String, String]): Long =
        configs.get(RetentionKey) match {
            case None    => NodeFilePathUtil.DefaultRetentionMs
            case Some(v) =>
                v.trim.toLongOption.filter(_ >= 0L).getOrElse {
                    throw new IllegalArgumentException(
                      s"$RetentionKey must be a whole number of milliseconds, 0 or more " +
                          s"(0 deletes a copy at once), got '$v'"
                    )
                }
        }

    /** Test-visible: sets the retention directly. */
    private[gbx] def setRetentionMs(ms: Long): Unit = NodeFilePathUtil.setRetentionMs(ms)

    /** Copies remote path to the node cache (with sidecars) unless a current copy exists, holds one
      * reader on it, and returns its local path. */
    def readRemote(remotePath: String): String = {
        val conf = hconf
        require(conf != null, "NodeFileManager.init must be called before readRemote")
        NodeFilePathUtil.readLock(remotePath, conf)._1
    }

    /** Releases one reader of the CURRENT copy of `remotePath`; at its last release the copy is kept
      * idle for the retention window, or deleted. A copy's local path is not a remote: release a copy
      * by its local path with `releaseLocal`. */
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
