package com.databricks.labs.gbx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.SerializableConfiguration

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

/** Caches remote paths as node-local copies via NodeFilePathUtil; each read holds one reader on a
  * copy until it is released (by remote, by local path, or by closing a lease). A copy no reader holds
  * is kept for the retention window, so the next read of an unchanged remote reuses it. */
object NodeFileManager extends Logging {

    /** Spark conf: how long a copy no read holds is kept for reuse, as a duration such as `60s` or `500ms`
      * (a bare number is milliseconds); 0 deletes it at once. Read from the ExpressionConfig passed to
      * `init` / `initForPlanning`. */
    val RetentionKey = "spark.databricks.labs.gbx.nodecache.retention.ms"

    @volatile private var hconf: SerializableConfiguration = _
    /** The invalid retention last warned about, so one bad setting is logged once per JVM, not per task. */
    @volatile private var warnedRetention: String = _

    /** Sets the Hadoop config used for copying; must be called before readRemote. Leaves the retention
      * as it is (the default, or what the last `init(conf)` in this JVM set). */
    def init(hadoopConf: SerializableConfiguration): Unit = {
        hconf = hadoopConf
    }

    /** Sets the Hadoop config used for copying and the retention from `conf`'s [[RetentionKey]]
      * (default 60 s; 0 deletes a copy at its last release). Executor-side, where every raster expression
      * runs it: an invalid value is logged and the default used, never thrown, so it cannot turn every
      * expression into an error. Reader planning refuses it instead (`initForPlanning`). */
    def init(conf: ExpressionConfig): Unit = {
        val ms = retentionOf(conf.configs).getOrElse {
            val v = conf.configs(RetentionKey)
            if (warnedRetention != v) {
                warnedRetention = v
                logWarning(s"${invalidRetention(v)}; using the default ${NodeFilePathUtil.DefaultRetentionMs} ms")
            }
            NodeFilePathUtil.DefaultRetentionMs
        }
        init(conf.hConf)
        NodeFilePathUtil.setRetentionMs(ms)
    }

    /** Driver-side reader planning: as `init(conf)`, but an invalid [[RetentionKey]] throws, so the read
      * fails with the reason before any task runs. */
    def initForPlanning(conf: ExpressionConfig): Unit = {
        if (retentionOf(conf.configs).isEmpty) {
            throw new IllegalArgumentException(invalidRetention(conf.configs(RetentionKey)))
        }
        init(conf)
    }

    /** The retention [[RetentionKey]] asks for (the default when unset), or None when its value is not a
      * duration of 0 or more. A Spark time string (`60s`, `500ms`, `2min`); a bare number is milliseconds. */
    private def retentionOf(configs: Map[String, String]): Option[Long] =
        configs.get(RetentionKey) match {
            case None    => Some(NodeFilePathUtil.DefaultRetentionMs)
            case Some(v) =>
                try Some(JavaUtils.timeStringAsMs(v)).filter(_ >= 0L)
                catch { case NonFatal(_) => None }
        }

    private def invalidRetention(v: String): String =
        s"$RetentionKey must be a duration of 0 or more, such as 60s or 500ms (a bare number is milliseconds; " +
            s"0 deletes a copy at once), got '$v'"

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
