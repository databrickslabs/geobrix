package com.databricks.labs.gbx.util

import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable

/** Caches remote paths to local copies via NodeFilePathUtil and tracks refs for release. */
object NodeFileManager {

    // remoteFilePath -> localFilePath
    private val local2Remote = mutable.Map[String, String]() // localFilePath -> remoteFilePath
    private var hconf: SerializableConfiguration = _

    /** Sets the Hadoop config used for copying; must be called before readRemote. */
    def init(hadoopConf: SerializableConfiguration): Unit = {
        hconf = hadoopConf
    }

    /** Copies remote path to local cache (with sidecars), acquires read lock, returns local path. */
    def readRemote(remotePath: String): String = {
        val (localPath, _) = NodeFilePathUtil.readLock(remotePath, hconf) // Create a read lock and make sure file exists
        // addJVMReadLock(remotePath, localPath) // Ensure that JVM has read locks count updated
        local2Remote.update(localPath, remotePath)
        localPath
    }

    /** Releases read lock for the given path (by local path key); may delete cache when refcount hits zero. */
    def releaseRemote(remotePath: String): Unit = {
        val remote = local2Remote.getOrElse(remotePath, "")
        NodeFilePathUtil.releaseReadLock(remote, hconf)
    }

}
