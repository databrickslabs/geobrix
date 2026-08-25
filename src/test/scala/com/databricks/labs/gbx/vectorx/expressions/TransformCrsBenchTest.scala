package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.bench.OnDemand
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
  * Structural design — two layers:
  *
  * 1. DETERMINISTIC CI guard (runs in every CI build, NOT tagged OnDemand):
  *    `ST_TransformCrs hot path ratio: end-to-end stays within inner-transform floor`
  *    Measures the inner OGR transform floor and the full eval path IN THE SAME JVM RUN,
  *    then asserts `endToEnd < innerFloor * 7`. Because both measurements share the same
  *    hardware and JVM state the RATIO is stable across CI runners — slow hardware scales
  *    BOTH numbers up together and the ratio holds. A per-row SpatialReference allocation
  *    regression adds ~fixed µs to the end-to-end path only, blowing the ratio on any
  *    hardware. K=7 leaves ~2–3x headroom over what we measure (ratio ≈ 2.0) and is still
  *    only one-third of what a per-row-alloc regression would cost (ratio ≈ 12+).
  *
  * 2. ABSOLUTE TIMING TESTS (tagged OnDemand, excluded from the default CI run):
  *    `TransformCrsCore inner OGR transform: measure µs/row after warmup` and
  *    `ST_TransformCrs.eval end-to-end: measure µs/row after warmup` measure 50000
  *    iterations each and print µs/row. These are useful on known hardware for tracking
  *    regression over time, but wall-clock thresholds are non-deterministic on shared CI
  *    runners (a CI run at 13.05 µs/row failed even though the code was correct). The OnDemand
  *    tag (excluded by pom.xml's `<tagsToExclude>`) keeps them out of the CI gate while
  *    preserving them for on-demand measurement via:
  *      gbx:test:scala --suite 'com.databricks.labs.gbx.vectorx.expressions.TransformCrsBenchTest'
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

    // ---------------------------------------------------------------------------
    // DETERMINISTIC CI GUARD — not tagged OnDemand; runs in every build.
    // ---------------------------------------------------------------------------

    test("ST_TransformCrs hot path ratio: end-to-end stays within inner-transform floor") {
        // --- warmup: fill caches and JIT-compile both paths ---
        val g = JTS.fromWKB(ewkb4326)
        val ct = SpatialRefOps.transformPlan("EPSG:4326", "EPSG:32633").transformation
        assert(ct != null, "EPSG:4326 -> EPSG:32633 must not be an identity plan")
        for (_ <- 0 until warmup) TransformCrsCore.transformWithCachedCT(g, ct)
        for (_ <- 0 until warmup) ST_TransformCrs.eval(ewkb4326, targetCrs)

        // --- measure inner floor ---
        val t0 = System.nanoTime()
        for (_ <- 0 until iters) TransformCrsCore.transformWithCachedCT(g, ct)
        val t1 = System.nanoTime()
        val innerFloorUs = (t1 - t0).toDouble / iters / 1000.0

        // --- measure end-to-end ---
        val t2 = System.nanoTime()
        for (_ <- 0 until iters) ST_TransformCrs.eval(ewkb4326, targetCrs)
        val t3 = System.nanoTime()
        val endToEndUs = (t3 - t2).toDouble / iters / 1000.0

        val ratio = endToEndUs / innerFloorUs
        println(s"[BENCH ratio-guard] inner floor: ${"%.2f".format(innerFloorUs)} µs/row, " +
            s"end-to-end: ${"%.2f".format(endToEndUs)} µs/row, ratio: ${"%.2f".format(ratio)}")

        // Ratio assertion: the full eval path must stay within 7x of the inner OGR floor.
        // Measured ratio is ≈ 2.0 on a dev container (1.7–2.2 across runs). K=7 gives ~3x
        // headroom over the observed ratio, yet is still only ≈ 0.4× the ratio seen when
        // per-row SpatialReference allocation was active (≈ 17 µs/row end-to-end with a
        // ≈ 1.6 µs/row inner floor → ratio ≈ 10–12). A regression that reintroduces
        // per-row alloc will blow this on any hardware.
        ratio should be < 7.0
    }

    // ---------------------------------------------------------------------------
    // ON-DEMAND ABSOLUTE TIMING TESTS — tagged OnDemand; excluded from CI by pom.xml.
    // Run directly via gbx:test:scala --suite 'TransformCrsBenchTest' for measurement.
    // ---------------------------------------------------------------------------

    test("TransformCrsCore inner OGR transform: measure µs/row after warmup", OnDemand) {
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
        // NOTE: this threshold is for on-demand tracking only, not a CI gate — see class docstring.
        usPerRow should be < 6.0
    }

    test("ST_TransformCrs.eval end-to-end: measure µs/row after warmup", OnDemand) {
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
        // NOTE: this threshold is for on-demand tracking only, not a CI gate — see class docstring.
        usPerRow should be < 10.0
    }
}
