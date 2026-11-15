package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.rasterx.operations.SpatialRefOps
import org.gdal.gdal.gdal
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
  * Direct tests of the SpatialRefOps.resolveCrs / crsToCanonical helpers (the int-cast rule and the
  * authority-else-WKT rule) and of the CRS the vector-raster bridge stamps. Requires GDAL native libs (run in Docker).
  */
class RST_CrsOpsTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
    }

    // --- SpatialRefOps.resolveCrs: the shared int-cast rule ---

    test("resolveCrs: int-castable string -> EPSG") {
        val sr = SpatialRefOps.resolveCrs("4326")
        SpatialRefOps.getEPSGCode(sr) shouldBe 4326
        sr.delete()
    }

    test("resolveCrs: whitespace-padded int-string -> EPSG") {
        val sr = SpatialRefOps.resolveCrs(" 32633 ")
        SpatialRefOps.getEPSGCode(sr) shouldBe 32633
        sr.delete()
    }

    test("resolveCrs: EPSG authority string -> EPSG") {
        val sr = SpatialRefOps.resolveCrs("EPSG:4326")
        SpatialRefOps.getEPSGCode(sr) shouldBe 4326
        sr.delete()
    }

    test("resolveCrs: non-EPSG ESRI authority string resolves (still valid, no EPSG code)") {
        val sr = SpatialRefOps.resolveCrs("ESRI:54008")
        sr should not be null
        SpatialRefOps.getEPSGCode(sr) shouldBe 0 // non-EPSG
        // Round-trips through crsToCanonical.
        SpatialRefOps.crsToCanonical(sr) shouldBe "ESRI:54008"
        sr.delete()
    }

    test("resolveCrs: WKT string resolves") {
        val ref = new SpatialReference()
        ref.ImportFromEPSG(4326)
        val wkt = ref.ExportToWkt()
        val sr = SpatialRefOps.resolveCrs(wkt)
        sr.IsSame(ref) shouldBe 1
        sr.delete(); ref.delete()
    }

    test("resolveCrs: garbage throws") {
        an[IllegalArgumentException] should be thrownBy SpatialRefOps.resolveCrs("not-a-crs-@@")
    }

    // --- crsToCanonical: authority-else-WKT ---

    test("crsToCanonical: EPSG SR -> 'EPSG:code'") {
        val sr = new SpatialReference()
        sr.ImportFromEPSG(4326)
        SpatialRefOps.crsToCanonical(sr) shouldBe "EPSG:4326"
        sr.delete()
    }

    test("crsToCanonical: null SR -> null") {
        SpatialRefOps.crsToCanonical(null) shouldBe null
    }

    test("VectorRasterBridge.buildEmptyRaster stamps an ESRI srid as ESRI (resolveCrs)") {
        import com.databricks.labs.gbx.rasterx.operations.SpatialRefOps
        import com.databricks.labs.gbx.rasterx.util.VectorRasterBridge
        // 54008 is an ESRI code — ImportFromEPSG auto-recovers it, and the bridge now
        // routes through resolveCrs, so the output raster carries ESRI:54008.
        val ds = VectorRasterBridge.buildEmptyRaster(0.0, 0.0, 1000.0, 1000.0, 8, 8, 54008)
        try SpatialRefOps.crsToCanonical(ds.GetSpatialRef) shouldBe "ESRI:54008"
        finally ds.delete()
    }

}
