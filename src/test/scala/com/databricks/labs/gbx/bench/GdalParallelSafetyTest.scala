package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.kernels.TestL1
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.gdal.gdal.gdal
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Paths}

/**
 * EXPERIMENT (local-first optimization of heavy spark-path): does concurrent GDAL in ONE
 * JVM SIGSEGV, or does GDALManager's synchronized/idempotent init make it safe?
 *
 * HeavyRunner.scala `coalesce(1)`s the spark-path DataFrame, citing a local[N] GDAL driver
 * race -- which kills ALL cluster parallelism (1000 tiles serial in one task). But the
 * PRODUCT runs heavy rst_* functions on multi-core executors (concurrent GDAL tasks in one
 * JVM) on clusters, so parallel GDAL must already be safe. This forces the exact concurrent
 * scenario in local[4]: if it completes, coalesce(1) is unnecessary and we can repartition
 * for parallelism.
 */
class GdalParallelSafetyTest extends AnyFunSuite {

  test("concurrent rst_fromcontent + column eval over repartitioned tiles (local[4]) does not crash") {
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("gdal-parallel-safety")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()
    try {
      GDALManager.loadSharedObjects(Iterable.empty[String])
      GDALManager.configureGDAL("/tmp", "/tmp", logCPL = false)
      gdal.AllRegister()
      TestL1.installAll(spark)

      val tif = this.getClass
        .getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        .toString.replace("file:/", "/")
      val bytes = Files.readAllBytes(Paths.get(tif))
      val dir = Files.createTempDirectory("gdal_par")
      // 16 copies so each of the 4 partitions opens several tiles -> sustained concurrent
      // GDAL Open across 4 task threads in the same JVM (what coalesce(1) prevented).
      val paths = (0 until 16).map { i =>
        val p = dir.resolve(s"t$i.tif"); Files.write(p, bytes); p.toString
      }

      val df = spark.read.format("binaryFile").load(paths: _*)
        .repartition(4) // FORCE 4 concurrent GDAL tasks (no coalesce(1))
        .withColumn("raster", functions.rst_fromcontent(col("content"), lit("GTiff")))
        .select(col("raster"))

      // Run a spark-path column over them concurrently; noop sink so it's pure compute.
      df.select(BenchDispatch.column("rst_width", col("raster"), Map.empty).alias("o"))
        .write.format("noop").mode("overwrite").save()

      // Reaching here = parallel GDAL in one JVM is safe; a driver race would have SIGSEGV'd
      // the JVM (crashing the suite), not thrown.
      val n = df.count()
      assert(n == 16L)
    } finally spark.stop()
  }

  test("parallel scaled groupBy().agg() (hash keys + broadcast group, no coalesce) is safe + correct") {
    // Mirrors the runAggregate fan-out fix: hash-partition keys into a bounded fan-out,
    // broadcast the small group, run a real groupBy(key).agg(combineavg) across concurrent
    // GDAL tasks in local[4]. Confirms the aggregate path is parallel-safe + produces a tile
    // per key (no SIGSEGV, no coalesce(1)).
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("gdal-parallel-agg")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()
    try {
      import org.apache.spark.sql.functions.broadcast
      GDALManager.loadSharedObjects(Iterable.empty[String])
      GDALManager.configureGDAL("/tmp", "/tmp", logCPL = false)
      gdal.AllRegister()
      TestL1.installAll(spark)

      val tif = this.getClass
        .getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        .toString.replace("file:/", "/")
      val bytes = Files.readAllBytes(Paths.get(tif))
      import spark.implicits._
      // 2 aligned copies = the combineavg group.
      val groupDf = spark.createDataFrame(Seq((0L, bytes), (1L, bytes))).toDF("cellid", "rb")
        .withColumn("tile", functions.rst_fromcontent(col("rb"), lit("GTiff")))
        .select(col("tile"))
        .cache()
      groupDf.count()

      val n = 24
      val parts = math.max(1, math.min(n, math.ceil(n.toDouble / 2).toInt)) // ~2 keys/task
      spark.conf.set("spark.sql.shuffle.partitions", parts.toString)
      val keys = spark.range(n).select(col("id").alias("key")).repartition(parts, col("key"))
      val scaled = keys.crossJoin(broadcast(groupDf)) // no coalesce(1)
      val ext = (0.0, 0.0, 0.0, 0.0, 0, 0, 0)
      val aggCol = BenchDispatch.aggregateColumn("rst_combineavg_agg", scaled, ext, Map.empty)
      val outRows = scaled.groupBy("key").agg(aggCol.alias("out")).count()
      assert(outRows == n.toLong, s"expected one aggregated tile per key: $outRows vs $n")
    } finally spark.stop()
  }
}
