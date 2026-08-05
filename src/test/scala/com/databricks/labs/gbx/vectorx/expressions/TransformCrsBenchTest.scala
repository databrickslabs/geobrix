package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Benchmark for ST_TransformCrs.eval after the getTransformerByCanonical optimization.
  *
  * Warms up with 2000 iterations, then measures 50000 iterations and prints µs/row.
  * The test always passes as long as the result is within a sanity bound (< 100 µs/row).
  * Run in Docker via: gbx:test:scala --suite 'com.databricks.labs.gbx.vectorx.expressions.TransformCrsBenchTest'
  */
class TransformCrsBenchTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
    }

    private val gf = new GeometryFactory()
    private val ewkb4326 = {
        val g = gf.createPoint(new Coordinate(11.0, 42.0))
        g.setSRID(4326)
        JTS.toEWKB(g)
    }
    private val targetCrs = UTF8String.fromString("EPSG:32633")

    test("TransformCrsCore benchmark: measure µs/row after warmup") {
        // Warmup — fills the transformer cache and JIT-compiles the hot path
        for (_ <- 0 until 2000) {
            ST_TransformCrs.eval(ewkb4326, targetCrs)
        }

        // Measure
        val iters = 50000
        val t0 = System.nanoTime()
        for (_ <- 0 until iters) {
            ST_TransformCrs.eval(ewkb4326, targetCrs)
        }
        val t1 = System.nanoTime()
        val µsPerRow = (t1 - t0).toDouble / iters / 1000.0

        // Print for the round-3 report — this line is captured by the task log
        println(s"[BENCH] ST_TransformCrs.eval: ${"%.2f".format(µsPerRow)} µs/row ($iters iters after 2000 warmup)")

        // Sanity: any reasonable result should be well under 100 µs/row
        µsPerRow should be < 100.0
    }
}
