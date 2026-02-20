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
}
