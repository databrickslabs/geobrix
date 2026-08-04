package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

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
}
