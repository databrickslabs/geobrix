package com.databricks.labs.gbx.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._

/** What the node-file cache holds on disk, for tests that check a read leaves no copy behind. */
object NodeFileCacheProbe {

    /** Every file and directory under `NodeFilePathUtil.rootPath`, relative to it. */
    def entries(): Set[String] = {
        val root = NodeFilePathUtil.rootPath
        if (!Files.exists(root)) Set.empty
        else {
            val s = Files.walk(root)
            try s.iterator().asScala.filter(_ != root).map(p => root.relativize(p).toString).toSet
            finally s.close()
        }
    }

    /** Bytes no GDAL or OGR driver recognises. */
    val garbage: Array[Byte] = ("not a dataset\n" * 64).getBytes(StandardCharsets.UTF_8)

    /** A fresh temp dir for `body`, deleted recursively afterwards. */
    def withScratchDir[T](prefix: String)(body: Path => T): T = {
        val dir = Files.createTempDirectory(prefix)
        try body(dir)
        finally deleteTree(dir)
    }

    /** A fresh temp dir holding a copy of the `tas_singlevar.nc` resource: `body(dir, nc)`. A fresh remote per
      * test, since the cache objects are JVM singletons. */
    def withNetCDFCopy[T](body: (Path, Path) => T): T = withScratchDir("gbx_nc_cache_") { dir =>
        val src = Paths.get(getClass.getResource("/binary/netcdf-singlevar/tas_singlevar.nc").toURI)
        val nc = Files.copy(src, dir.resolve("tas_singlevar.nc"))
        body(dir, nc)
    }

    def deleteTree(p: Path): Unit =
        if (Files.exists(p)) {
            val s = Files.walk(p)
            try s.sorted(java.util.Comparator.reverseOrder[Path]()).iterator().asScala.foreach(q => Files.deleteIfExists(q))
            finally s.close()
        }

}

/** For a suite that checks a read leaves no node-file copy behind: copies are freed at their last release
  * (no idle retention), both in this JVM and through the session conf that the readers apply. A test that
  * wants retention sets both back for its own duration. The retention this JVM had before the suite is
  * restored after it. */
trait NodeCacheRetentionOff extends SharedSparkSession {

    private var priorRetentionMs: Long = NodeFilePathUtil.DefaultRetentionMs

    override protected def sparkConf: SparkConf = super.sparkConf.set(NodeFileManager.RetentionKey, "0")

    override def beforeAll(): Unit = {
        priorRetentionMs = NodeFilePathUtil.retentionMs
        NodeFileManager.setRetentionMs(0L)
        super.beforeAll()
    }

    override def afterAll(): Unit =
        try super.afterAll()
        finally NodeFileManager.setRetentionMs(priorRetentionMs)

    /** Runs `body` with the session conf's retention set to `value` (as a user would set it), then restores
      * the session conf and this JVM's retention as they were. */
    protected def withSessionRetention[T](value: String)(body: => T): T = {
        val priorConf = spark.conf.getOption(NodeFileManager.RetentionKey)
        val priorMs = NodeFilePathUtil.retentionMs
        spark.conf.set(NodeFileManager.RetentionKey, value)
        try body
        finally {
            priorConf match {
                case Some(v) => spark.conf.set(NodeFileManager.RetentionKey, v)
                case None    => spark.conf.unset(NodeFileManager.RetentionKey)
            }
            NodeFileManager.setRetentionMs(priorMs)
        }
    }

    /** The messages of `t` and its causes. */
    protected def messageChain(t: Throwable): Seq[String] =
        Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(16).map(e => String.valueOf(e.getMessage)).toSeq

}
