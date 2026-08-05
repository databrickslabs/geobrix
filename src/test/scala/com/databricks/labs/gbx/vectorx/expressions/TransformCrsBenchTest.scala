package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.operations.SpatialRefOps
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Regression guard for the ST_TransformCrs hot path.
  *
  * Measures 50000 iterations after 3000 warmup and prints µs/row for BOTH the inner OGR
  * transform helper and the full `ST_TransformCrs.eval` path that SQL actually runs.
  *
  * The bound exists to catch the specific regression class this path has already suffered
  * twice: reintroducing per-row `SpatialReference` allocation (per-row `resolveCrs` cost
  * ~3.5 µs for EPSG:4326 and ~8 µs for EPSG:32633, i.e. ~17 µs/row for the two ends). The
  * bound is therefore set well below the old per-row-resolve cost so any such regression
  * fails, while leaving generous headroom over the measured value for slower CI hardware.
  *
  * Run in Docker via:
  * gbx:test:scala --suite 'com.databricks.labs.gbx.vectorx.expressions.TransformCrsBenchTest'
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

    private val iters = 50000
    private val warmup = 3000

    test("TransformCrsCore inner OGR transform: measure µs/row after warmup") {
        val g = JTS.fromWKB(ewkb4326)
        val ct = SpatialRefOps.transformPlan("EPSG:4326", "EPSG:32633").transformation
        assert(ct != null, "EPSG:4326 -> EPSG:32633 must not be an identity plan")

        for (_ <- 0 until warmup) TransformCrsCore.transformWithCachedCT(g, ct)

        val t0 = System.nanoTime()
        for (_ <- 0 until iters) TransformCrsCore.transformWithCachedCT(g, ct)
        val t1 = System.nanoTime()
        val usPerRow = (t1 - t0).toDouble / iters / 1000.0

        println(s"[BENCH] TransformCrsCore.transformWithCachedCT: " +
            s"${"%.2f".format(usPerRow)} µs/row ($iters iters after $warmup warmup)")

        // Measured 1.58 and 1.72 µs/row across two dev-container runs (the OGR WKB round-trip
        // floor alone is ~0.85 µs/row). 6.0 leaves ~3.5x headroom for slower CI hardware while
        // still failing if per-row SpatialReference allocation creeps back into this helper.
        usPerRow should be < 6.0
    }

    test("ST_TransformCrs.eval end-to-end: measure µs/row after warmup") {
        // Warmup — fills the CrsInfo / transform-plan caches and JIT-compiles the hot path
        for (_ <- 0 until warmup) ST_TransformCrs.eval(ewkb4326, targetCrs)

        val t0 = System.nanoTime()
        for (_ <- 0 until iters) ST_TransformCrs.eval(ewkb4326, targetCrs)
        val t1 = System.nanoTime()
        val usPerRow = (t1 - t0).toDouble / iters / 1000.0

        println(s"[BENCH] ST_TransformCrs.eval: " +
            s"${"%.2f".format(usPerRow)} µs/row ($iters iters after $warmup warmup)")

        // Measured 2.35 and 3.31 µs/row across two dev-container runs. 10.0 leaves ~3x headroom
        // over the worse observation for slower CI hardware, yet is only half the 19.06 µs/row
        // this path cost while it allocated two SpatialReference objects per row — so that
        // regression (or anything within 2x of it) cannot pass silently again.
        usPerRow should be < 10.0
    }
}
