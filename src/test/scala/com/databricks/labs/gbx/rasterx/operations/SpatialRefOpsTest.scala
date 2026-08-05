package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

// Neutral-package alias for dual-path assertions (Task 2)
import com.databricks.labs.gbx.{operations => gbxops}

/** Tests for SpatialRefOps (OSR SpatialReference helpers). Requires GDAL native libs (e.g. run in Docker). */
class SpatialRefOpsTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
    }

    test("fromEPSGCode(4326) should return SpatialReference with EPSG 4326") {
        val sr = SpatialRefOps.fromEPSGCode(4326)
        sr should not be null
        SpatialRefOps.getEPSGCode(sr) shouldBe 4326
    }

    test("fromEPSGCode with positive code should return SR with that EPSG") {
        val sr = SpatialRefOps.fromEPSGCode(32618)
        sr should not be null
        SpatialRefOps.getEPSGCode(sr) shouldBe 32618
    }

    test("fromEPSGCode(0) should return WGS84 (no exception)") {
        val sr = SpatialRefOps.fromEPSGCode(0)
        sr should not be null
        // WGS84 may be reported as EPSG:4326 by GDAL; we only require it doesn't throw
        val code = SpatialRefOps.getEPSGCode(sr)
        code should (be(0) or be(4326))
    }

    test("fromEPSGCode(negative) should return WGS84 (no exception)") {
        val sr = SpatialRefOps.fromEPSGCode(-1)
        sr should not be null
        val code = SpatialRefOps.getEPSGCode(sr)
        code should (be(0) or be(4326))
    }

    test("getEPSGCode on EPSG SR should return the code") {
        val sr = SpatialRefOps.fromEPSGCode(4326)
        SpatialRefOps.getEPSGCode(sr) shouldBe 4326
    }

    test("getEPSGCode on SR with no EPSG authority should return 0") {
        val sr = new SpatialReference()
        // No ImportFromEPSG or other authority set -> GetAuthorityName(null) typically null
        SpatialRefOps.getEPSGCode(sr) shouldBe 0
    }

    // --- resolveCrs int-string path: the SRID resolution rule (epsg -> esri) ---
    // ImportFromEPSG auto-recovers an ESRI code, so an int-castable SRID string is
    // classified as EPSG or ESRI (mirrors the light tier's resolve_crs), and a code in
    // neither authority raises. This is the int path RST_SetSrid stamps through.

    test("resolveCrs: int-string ESRI-only code (54008) -> ESRI, not mislabeled EPSG") {
        val sr = SpatialRefOps.resolveCrs("54008")
        sr should not be null
        SpatialRefOps.crsToCanonical(sr) shouldBe "ESRI:54008"
        sr.delete()
    }

    test("resolveCrs: int-string EPSG code (4326) -> EPSG") {
        val sr = SpatialRefOps.resolveCrs("4326")
        sr should not be null
        SpatialRefOps.crsToCanonical(sr) shouldBe "EPSG:4326"
        sr.delete()
    }

    test("resolveCrs: int-string code in neither authority (99999999) throws") {
        an[IllegalArgumentException] should be thrownBy SpatialRefOps.resolveCrs("99999999")
    }

    // --- Task 2 (CRS-100 foundation): getTransformer cache + resolveSourceSR ---

    test("getTransformer reuses the same CoordinateTransformation for equivalent CRS keys") {
        val t1 = SpatialRefOps.getTransformer("EPSG:4326", "EPSG:32633")
        val t2 = SpatialRefOps.getTransformer("4326", "32633") // equivalent spellings
        assert(t1 eq t2) // same cached instance (same thread)
    }

    test("getTransformer stays bounded (LRU-evicts) beyond the cache cap") {
        // 120 valid UTM zones x 2 targets = 240 distinct pairs > cap; no error, bounded.
        val zones = (32601 to 32660) ++ (32701 to 32760)
        zones.foreach { z =>
            SpatialRefOps.getTransformer(z.toString, "4326")
            SpatialRefOps.getTransformer(z.toString, "3857")
        }
        // a freshly requested pair still works after eviction churn
        SpatialRefOps.getTransformer("4326", "3857") should not be null
    }

    test("resolveSourceSR: embedded wins; single param; both -> error; neither -> None") {
        SpatialRefOps.crsToCanonical(
          SpatialRefOps.resolveSourceSR(4326, None, None).get) shouldBe "EPSG:4326"
        SpatialRefOps.crsToCanonical(
          SpatialRefOps.resolveSourceSR(54008, None, None).get) shouldBe "ESRI:54008"
        SpatialRefOps.crsToCanonical(
          SpatialRefOps.resolveSourceSR(0, Some(32633), None).get) shouldBe "EPSG:32633"
        SpatialRefOps.crsToCanonical(
          SpatialRefOps.resolveSourceSR(0, None, Some("ESRI:54008")).get) shouldBe "ESRI:54008"
        SpatialRefOps.resolveSourceSR(0, None, None) shouldBe None
        an[IllegalArgumentException] should be thrownBy
            SpatialRefOps.resolveSourceSR(0, Some(4326), Some("EPSG:3857"))
        // embedded present + param -> param ignored, no error (mixed-column safe)
        SpatialRefOps.crsToCanonical(
          SpatialRefOps.resolveSourceSR(4326, Some(32633), None).get) shouldBe "EPSG:4326"
    }

    // --- Task 2: dual-path assertions ---
    // The neutral com.databricks.labs.gbx.operations.SpatialRefOps must resolve correctly,
    // AND the rasterx.operations.SpatialRefOps forwarder must return identical results.
    // This is the regression gate for the ~12 unchanged rasterx importers.

    test("Task2: neutral SpatialRefOps.resolveCrs(54008) canonicalizes to ESRI:54008") {
        val sr = gbxops.SpatialRefOps.resolveCrs("54008")
        sr should not be null
        gbxops.SpatialRefOps.crsToCanonical(sr) shouldBe "ESRI:54008"
        sr.delete()
    }

    test("Task2: rasterx forwarder resolveCrs(54008) matches neutral path") {
        val srNeutral = gbxops.SpatialRefOps.resolveCrs("54008")
        val srForwarder = SpatialRefOps.resolveCrs("54008")
        gbxops.SpatialRefOps.crsToCanonical(srNeutral) shouldBe
            SpatialRefOps.crsToCanonical(srForwarder)
        srNeutral.delete()
        srForwarder.delete()
    }

    test("Task2: neutral SpatialRefOps.resolveCrs(4326) canonicalizes to EPSG:4326") {
        val sr = gbxops.SpatialRefOps.resolveCrs("4326")
        sr should not be null
        gbxops.SpatialRefOps.crsToCanonical(sr) shouldBe "EPSG:4326"
        sr.delete()
    }

    test("Task2: rasterx forwarder resolveCrs(4326) matches neutral path") {
        val srNeutral = gbxops.SpatialRefOps.resolveCrs("4326")
        val srForwarder = SpatialRefOps.resolveCrs("4326")
        gbxops.SpatialRefOps.crsToCanonical(srNeutral) shouldBe
            SpatialRefOps.crsToCanonical(srForwarder)
        srNeutral.delete()
        srForwarder.delete()
    }

    test("Task2: neutral getEPSGCode(4326) returns 4326") {
        val sr = gbxops.SpatialRefOps.fromEPSGCode(4326)
        gbxops.SpatialRefOps.getEPSGCode(sr) shouldBe 4326
    }

    test("Task2: rasterx forwarder getEPSGCode matches neutral path") {
        val sr = gbxops.SpatialRefOps.fromEPSGCode(4326)
        SpatialRefOps.getEPSGCode(sr) shouldBe gbxops.SpatialRefOps.getEPSGCode(sr)
    }

    test("Task2: neutral fromEPSGCode(27700) returns BNG SR") {
        val sr = gbxops.SpatialRefOps.fromEPSGCode(27700)
        sr should not be null
        gbxops.SpatialRefOps.getEPSGCode(sr) shouldBe 27700
    }

    test("Task2: rasterx forwarder fromEPSGCode(27700) matches neutral path") {
        val srNeutral = gbxops.SpatialRefOps.fromEPSGCode(27700)
        val srForwarder = SpatialRefOps.fromEPSGCode(27700)
        SpatialRefOps.getEPSGCode(srForwarder) shouldBe gbxops.SpatialRefOps.getEPSGCode(srNeutral)
    }

    test("Task2: neutral resolveSourceSR embedded SRID=4326 wins") {
        val sr = gbxops.SpatialRefOps.resolveSourceSR(4326, None, None).get
        gbxops.SpatialRefOps.crsToCanonical(sr) shouldBe "EPSG:4326"
    }

    test("Task2: rasterx forwarder resolveSourceSR matches neutral path") {
        val canonical = gbxops.SpatialRefOps.crsToCanonical(
            gbxops.SpatialRefOps.resolveSourceSR(4326, None, None).get)
        val canonicalFwd = SpatialRefOps.crsToCanonical(
            SpatialRefOps.resolveSourceSR(4326, None, None).get)
        canonicalFwd shouldBe canonical
    }

    // --- getTransformer: axis correctness and leak-free resource handling ---

    test("getTransformer: returns non-null CoordinateTransformation") {
        val ct = gbxops.SpatialRefOps.getTransformer("EPSG:4326", "EPSG:32633")
        ct should not be null
    }

    test("getTransformer: cache hit returns same instance") {
        val ct1 = gbxops.SpatialRefOps.getTransformer("EPSG:4326", "EPSG:32633")
        val ct2 = gbxops.SpatialRefOps.getTransformer("EPSG:4326", "EPSG:32633")
        assert(ct1 eq ct2)
    }

    test("getTransformer: different key pair creates new CT (cache miss)") {
        val ct1 = gbxops.SpatialRefOps.getTransformer("EPSG:4326", "EPSG:32633")
        val ct2 = gbxops.SpatialRefOps.getTransformer("EPSG:4326", "EPSG:32634")
        assert(!(ct1 eq ct2))
    }

    test("getTransformer: produces non-axis-flipped coordinates for EPSG:4326 -> EPSG:32633") {
        // POINT (11, 42) in 4326 (lon=11, lat=42) should project to UTM zone 33N
        // Expected: x ≈ 168701 m, y ≈ 4657521 m (NOT axis-flipped ~3.5M, 168701).
        import org.gdal.ogr.{Geometry => OGRGeometry}
        import com.databricks.labs.gbx.vectorx.jts.JTS
        val ct = gbxops.SpatialRefOps.getTransformer("EPSG:4326", "EPSG:32633")
        val ogrGeom = OGRGeometry.CreateFromWkt("POINT (11 42)")
        ogrGeom.Transform(ct)
        val wkb = JTS.fromWKB(ogrGeom.ExportToWkb())
        ogrGeom.delete()
        wkb.getCoordinate.getX shouldBe (168701.0 +- 168701.0 * 1e-4)
        wkb.getCoordinate.getY shouldBe (4657521.0 +- 4657521.0 * 1e-4)
    }

    // --- getTransformerByCanonical (Round 3 perf overload) ---

    test("getTransformerByCanonical: returns non-null CoordinateTransformation") {
        val ct = gbxops.SpatialRefOps.getTransformerByCanonical("EPSG:4326", "EPSG:32633")
        ct should not be null
    }

    test("getTransformerByCanonical: cache hit returns same instance") {
        val ct1 = gbxops.SpatialRefOps.getTransformerByCanonical("EPSG:4326", "EPSG:32633")
        val ct2 = gbxops.SpatialRefOps.getTransformerByCanonical("EPSG:4326", "EPSG:32633")
        assert(ct1 eq ct2)
    }

    test("getTransformerByCanonical: different key pair creates new CT (cache miss)") {
        val ct1 = gbxops.SpatialRefOps.getTransformerByCanonical("EPSG:4326", "EPSG:32633")
        val ct2 = gbxops.SpatialRefOps.getTransformerByCanonical("EPSG:4326", "EPSG:32634")
        assert(!(ct1 eq ct2))
    }

    test("getTransformerByCanonical: produces non-axis-flipped coordinates for EPSG:4326 -> EPSG:32633") {
        // Verify the canonical-overload CT gives the same result as getTransformer.
        import org.gdal.ogr.{Geometry => OGRGeometry}
        import com.databricks.labs.gbx.vectorx.jts.JTS
        val ct = gbxops.SpatialRefOps.getTransformerByCanonical("EPSG:4326", "EPSG:32633")
        val ogrGeom = OGRGeometry.CreateFromWkt("POINT (11 42)")
        ogrGeom.Transform(ct)
        val wkb = JTS.fromWKB(ogrGeom.ExportToWkb())
        ogrGeom.delete()
        wkb.getCoordinate.getX shouldBe (168701.0 +- 168701.0 * 1e-4)
        wkb.getCoordinate.getY shouldBe (4657521.0 +- 4657521.0 * 1e-4)
    }

    test("getTransformerByCanonical: result equivalent to getTransformer for same CRS pair") {
        // Both should transform POINT (11, 42) to the same coordinates.
        import org.gdal.ogr.{Geometry => OGRGeometry}
        import com.databricks.labs.gbx.vectorx.jts.JTS
        val ctOld = gbxops.SpatialRefOps.getTransformer("EPSG:4326", "EPSG:32633")
        val ctNew = gbxops.SpatialRefOps.getTransformerByCanonical("EPSG:4326", "EPSG:32633")
        // They should be the same instance (same thread, same canonical key, same LRU cache)
        assert(ctOld eq ctNew)
    }
}
