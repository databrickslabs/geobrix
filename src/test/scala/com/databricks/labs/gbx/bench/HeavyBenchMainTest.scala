package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.gdal.gdal
import java.nio.file.{Files, Paths}

class HeavyBenchMainTest extends PlanTest with SilentSparkSession {
  test("HeavyBenchMain.run writes a heavyweight shard (pure-core) from primitive args") {
    GDALManager.loadSharedObjects(Iterable.empty[String])
    GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
    gdal.AllRegister()
    val tif = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
      .toString.replace("file:/", "/")
    val dir = Files.createTempDirectory("c2")
    val json = s"""{"seed":1,"size_sweep":[{"path":"$tif","cellid":0,"srid":4326,"dtype":"float32","bands":1,"tile_px":1200,"nodata_frac":0.0}],"row_pool":{"tile_px":1200,"bands":1,"dtype":"float32","tiles":[]}}"""
    Files.write(dir.resolve("corpus.json"), json.getBytes)
    val out = dir.resolve("heavyweight.jsonl").toString

    // local test: corpusRoot == localCorpusRoot (no Volume), so pure-core reads dir directly.
    HeavyBenchMain.run(spark, dir.toString, dir.toString, out, "rst_width,rst_height", "pure-core", "2,4", 1, 2, "c2")

    val lines = Files.readAllLines(Paths.get(out)).toArray
    assert(lines.length == 2)
    assert(lines.forall(_.toString.contains("\"api\":\"heavyweight\"")))
  }
}
