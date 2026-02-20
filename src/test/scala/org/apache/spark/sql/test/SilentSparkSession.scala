package org.apache.spark.sql.test

import scala.jdk.CollectionConverters._

import com.databricks.labs.gbx.rasterx.{ErrorTokenListener, ProjErrorFilter}
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator

trait SilentSparkSession extends SharedSparkSession {

  /** Extra thread-name patterns to exclude from the leak detector (Spark executor/shuffle threads). */
  private val sparkExecutorThreadExcludes = Set(
    "QueryStageCreator.*",
    "ResultQueryStageExecution.*",
    "shuffle-boss.*",
    "shuffle-exchange.*",
    "files-client.*",
    "rpc-boss.*",
    "Cleaner-.*",
    "ForkJoinPool\\.commonPool-worker.*",
    ".*NativeObjectsCleaner"
  )

  private var ourThreadSnapshot: Set[String] = Set.empty

  override def beforeAll(): Unit = {
    ourThreadSnapshot = Thread.getAllStackTraces.keySet.asScala.toSet[Thread].map(_.getName)
    super.beforeAll()
    ErrorTokenListener.install(ProjErrorFilter.TOKEN)
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null && !spark.sparkContext.isStopped) {
        spark.stop()
      }
    } finally {
      super.afterAll()
    }
  }

  /** Same as Spark's post-audit but excludes known Spark executor/shuffle threads. */
  override protected def doThreadPostAudit(): Unit = {
    val shortSuiteName = this.getClass.getName.replaceAll("org.apache.spark", "o.a.s")
    if (ourThreadSnapshot.nonEmpty) {
      val remainingThreads = Thread.getAllStackTraces.keySet.asScala.toSet
        .filterNot(t => ourThreadSnapshot.contains(t.getName))
        .filterNot(t => threadExcludeList.exists(t.getName.matches(_)))
        .filterNot(t => sparkExecutorThreadExcludes.exists(t.getName.matches(_)))
      if (remainingThreads.nonEmpty) {
        logWarning(
          s"\n\n===== POSSIBLE THREAD LEAK IN SUITE $shortSuiteName, " +
            s"threads: ${remainingThreads.map(t => s"${t.getName} (daemon=${t.isDaemon})").mkString(", ")} =====\n"
        )
      }
    } else {
      logWarning(
        s"\n\n===== THREAD AUDIT POST ACTION CALLED WITHOUT PRE ACTION IN SUITE $shortSuiteName =====\n"
      )
    }
  }

  override def createSparkSession: TestSparkSession = {
        Configurator.setRootLevel(Level.WARN)
        Configurator.setLevel("org.apache.spark", Level.ERROR)
        Configurator.setLevel("org.sparkproject", Level.ERROR)
        Configurator.setLevel("org.eclipse.jetty", Level.WARN)
        val conf = sparkConf
        conf.set("spark.driver.extraJavaOptions", "-Djava.library.path=/usr/local/hadoop/lib/native")
        conf.set("spark.executor.extraJavaOptions", "-Djava.library.path=/usr/local/hadoop/lib/native")
        conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        conf.set("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
        conf.set("spark.hadoop.fs.file.impl.disable.cache", "true")
        conf.set("spark.default.parallelism", "8")
        conf.set("spark.sql.shuffle.partitions", "8")
        conf.set("spark.sql.adaptive.enabled", "false")
        conf.set("spark.ui.enabled", "false")
        val session = new TestSparkSession(conf, 1, 12)
        session.sparkContext.setLogLevel("ERROR")
        session
    }
}
