package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.SparkSession
import org.gdal.gdal.gdal
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import java.nio.file.{Files, Paths}

class HeavyRunnerTest extends AnyFunSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    GDALManager.loadSharedObjects(Iterable.empty[String])
    GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
    gdal.AllRegister()
  }

  test("pure-core run over a tiny corpus yields ok heavyweight rows with fingerprints") {
    val tif = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
      .toString.replace("file:/", "/")
    val dir = Files.createTempDirectory("corpus")
    val json =
      s"""{"seed":1,"size_sweep":[
         | {"path":"$tif","cellid":0,"srid":4326,"dtype":"float32","bands":1,"tile_px":1200,"nodata_frac":0.0}],
         | "row_pool":{"tile_px":1200,"bands":1,"dtype":"float32","tiles":[]}}""".stripMargin
    Files.write(dir.resolve("corpus.json"), json.getBytes)

    val rows = HeavyRunner.runPureCore(
      corpusRoot = dir.toString,
      corpus = BenchManifest.read(dir.resolve("corpus.json").toString),
      fns = Seq("rst_width", "rst_numbands"),
      runId = "t", warmup = 1, measured = 2, argsByFn = Map.empty)

    assert(rows.nonEmpty)
    assert(rows.forall(_.api == "heavyweight"))
    val byFn = rows.groupBy(_.fn)
    assert(byFn("rst_width").head.status == "ok")
    assert(byFn("rst_width").head.output_fingerprint.contains("scalar"))
  }

  test("sink is invoked per row and JsonlAppender flushes byte-identical lines") {
    val tif = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
      .toString.replace("file:/", "/")
    val dir = Files.createTempDirectory("corpus")
    val json =
      s"""{"seed":1,"size_sweep":[
         | {"path":"$tif","cellid":0,"srid":4326,"dtype":"float32","bands":1,"tile_px":1200,"nodata_frac":0.0}],
         | "row_pool":{"tile_px":1200,"bands":1,"dtype":"float32","tiles":[]}}""".stripMargin
    Files.write(dir.resolve("corpus.json"), json.getBytes)
    val corpus = BenchManifest.read(dir.resolve("corpus.json").toString)
    val fns = Seq("rst_width", "rst_numbands")

    // Sink fires once per produced row, in order.
    val sunk = scala.collection.mutable.ArrayBuffer.empty[BenchRow]
    val rows = HeavyRunner.runPureCore(
      corpusRoot = dir.toString, corpus = corpus, fns = fns,
      runId = "t", warmup = 1, measured = 2, argsByFn = Map.empty, sink = sunk += _)
    assert(sunk.toSeq == rows, "every row must be handed to the sink, in order")

    // JsonlAppender produces exactly the same bytes as the bulk writeJsonl.
    val incPath = dir.resolve("inc.jsonl").toString
    val bulkPath = dir.resolve("bulk.jsonl").toString
    val w = BenchIO.appendWriter(incPath)
    try rows.foreach(w.append) finally w.close()
    BenchIO.writeJsonl(rows, bulkPath)
    val incBytes = Files.readAllBytes(java.nio.file.Paths.get(incPath))
    val bulkBytes = Files.readAllBytes(java.nio.file.Paths.get(bulkPath))
    assert(java.util.Arrays.equals(incBytes, bulkBytes),
      "incremental append must be byte-identical to bulk writeJsonl")
    // Each line is one valid row (count matches).
    val lines = new String(incBytes, java.nio.charset.StandardCharsets.UTF_8)
      .split("\n").filter(_.nonEmpty)
    assert(lines.length == rows.length)
  }

  test("aggParts: all kinds parallelize on the same bounded fan-out (~2 keys/task, capped at n)") {
    // ceil(n/2) keys/task, capped at n; large-output aggs stay bounded per task.
    assert(HeavyRunner.aggParts("tile_aggregate", 1000) == 500)
    assert(HeavyRunner.aggParts("tile_aggregate", 4) == 2)
    assert(HeavyRunner.aggParts("tile_aggregate", 1) == 1)
    // Geometry aggregators are no longer pinned to 1 -- the GDAL/OGR registration race is closed
    // (GDALManager.init/initOgr), so they parallelize identically to tile aggregators.
    assert(HeavyRunner.aggParts("geometry_aggregate", 1000) == 500)
    assert(HeavyRunner.aggParts("geometry_aggregate", 10) == 5)
    assert(HeavyRunner.aggParts("geometry_aggregate", 1) == 1)
  }

  test("timeIters runs the warm body for warm-up and the measured body for measured") {
    // The warm-up iterations run warmBody (the cheap, minimal-data stand-in); the measured
    // iterations run body (the full job). Counts must match warmup/measured exactly so the
    // warm-up never charges the full-N cost on the measured iterations.
    var warmCount = 0
    var bodyCount = 0
    val warmBody = () => { warmCount += 1 }
    val body = () => { bodyCount += 1 }
    HeavyRunner.timeIters(body, warmup = 2, measured = 3, warmBody = warmBody)
    assert(warmCount == 2, s"warm body should run warmup(2) times, got $warmCount")
    assert(bodyCount == 3, s"measured body should run measured(3) times, got $bodyCount")

    // With no warm body (null), the warm-up falls back to running body itself.
    var only = 0
    HeavyRunner.timeIters(() => { only += 1 }, warmup = 2, measured = 3)
    assert(only == 5, s"with no warm body, body runs warmup+measured(5) times, got $only")
  }

  test("spark-path run parallelizes (no coalesce) over a row_pool and yields ok rows") {
    // Validates the repartition-for-parallelism change in runSparkPath: a local[4] session
    // runs the heavy spark-path column over an 8-tile row_pool. With the old coalesce(1) this
    // ran on one task; now it repartitions, so concurrent GDAL tasks must stay safe (they do
    // via GDALManager's synchronized init) and the rows come back ok.
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("heavy-sparkpath")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()
    try {
      val tif = this.getClass
        .getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        .toString.replace("file:/", "/")
      val bytes = Files.readAllBytes(Paths.get(tif))
      val dir = Files.createTempDirectory("rowpool")
      val tilesJson = (0 until 8).map { i =>
        Files.write(dir.resolve(s"r$i.tif"), bytes)
        s"""{"path":"r$i.tif","cellid":$i,"srid":4326,"dtype":"float32","bands":1,"tile_px":1200,"nodata_frac":0.0}"""
      }.mkString(",")
      val json =
        s"""{"seed":1,"size_sweep":[],"row_pool":{"tile_px":1200,"bands":1,"dtype":"float32","tiles":[$tilesJson]}}"""
      Files.write(dir.resolve("corpus.json"), json.getBytes)
      val corpus = BenchManifest.read(dir.resolve("corpus.json").toString)

      val rows = HeavyRunner.runSparkPath(
        spark, dir.toString, corpus, fns = Seq("rst_width"),
        runId = "t", rowCounts = Seq(4, 8), warmup = 1, measured = 1, argsByFn = Map.empty)

      assert(rows.nonEmpty)
      assert(rows.filter(_.fn == "rst_width").forall(_.status == "ok"),
        s"all rst_width spark-path rows should be ok: ${rows.map(r => r.fn -> r.status)}")
      assert(rows.exists(_.rows == 8L))
    } finally spark.stop()
  }
}
