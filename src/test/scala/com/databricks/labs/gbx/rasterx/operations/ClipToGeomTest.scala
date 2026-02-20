package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class ClipToGeomTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString.replace("file:/", "/")
        ds = gdal.Open(tifPath)
    }

    override def afterAll(): Unit = {
        ds.delete()
    }

    test("ClipToGeom should clip raster to geometry") {
        // Create a polygon that intersects the raster
        // MODIS tile is in EPSG:32610 (UTM Zone 10N)
        val wkt = "POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"
        val geom = JTS.fromWKT(wkt)
        
        val geomSR = new SpatialReference()
        geomSR.ImportFromEPSG(32610)

        val (resultDs, metadata) = ClipToGeom.clip(ds, Map.empty, geom, geomSR, cutlineAllTouched = true)

        resultDs should not be null
        resultDs.GetRasterCount shouldBe ds.GetRasterCount
        // Clipped raster should be smaller than original
        resultDs.GetRasterXSize should be <= ds.GetRasterXSize
        resultDs.GetRasterYSize should be <= ds.GetRasterYSize

        metadata should not be null
        metadata should contain key "path"

        resultDs.delete()
        geomSR.delete()
    }

    test("ClipToGeom should handle small geometries") {
        // Create a very small polygon (smaller than pixel size)
        val wkt = "POLYGON((-8895604 2223901, -8895604 2223900, -8895603 2223900, -8895603 2223901, -8895604 2223901))"
        val geom = JTS.fromWKT(wkt)
        
        val geomSR = new SpatialReference()
        geomSR.ImportFromEPSG(32610)

        val (resultDs, _) = ClipToGeom.clip(ds, Map.empty, geom, geomSR, cutlineAllTouched = true)

        resultDs should not be null
        // Should still produce a result even for small geometries (buffered)
        resultDs.GetRasterXSize should be > 0
        resultDs.GetRasterYSize should be > 0

        resultDs.delete()
        geomSR.delete()
    }

    test("ClipToGeom should respect cutlineAllTouched parameter") {
        val wkt = "POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"
        val geom = JTS.fromWKT(wkt)
        
        val geomSR = new SpatialReference()
        geomSR.ImportFromEPSG(32610)

        val (resultTrue, _) = ClipToGeom.clip(ds, Map.empty, geom, geomSR, cutlineAllTouched = true)
        val (resultFalse, _) = ClipToGeom.clip(ds, Map.empty, geom, geomSR, cutlineAllTouched = false)

        resultTrue should not be null
        resultFalse should not be null

        // Both should produce valid results
        resultTrue.GetRasterXSize should be > 0
        resultFalse.GetRasterXSize should be > 0

        resultTrue.delete()
        resultFalse.delete()
        geomSR.delete()
    }

    test("ClipToGeom should preserve band count") {
        val wkt = "POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"
        val geom = JTS.fromWKT(wkt)
        
        val geomSR = new SpatialReference()
        geomSR.ImportFromEPSG(32610)

        val originalBandCount = ds.GetRasterCount

        val (resultDs, _) = ClipToGeom.clip(ds, Map.empty, geom, geomSR, cutlineAllTouched = true)

        resultDs.GetRasterCount shouldBe originalBandCount

        resultDs.delete()
        geomSR.delete()
    }

    test("ClipToGeom should preserve spatial reference") {
        val wkt = "POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"
        val geom = JTS.fromWKT(wkt)
        
        val geomSR = new SpatialReference()
        geomSR.ImportFromEPSG(32610)

        val originalSR = ds.GetSpatialRef
        val originalEPSG = originalSR.GetAuthorityCode(null)

        val (resultDs, _) = ClipToGeom.clip(ds, Map.empty, geom, geomSR, cutlineAllTouched = true)

        val resultSR = resultDs.GetSpatialRef
        val resultEPSG = resultSR.GetAuthorityCode(null)

        resultEPSG shouldBe originalEPSG

        resultDs.delete()
        geomSR.delete()
    }

    test("ClipToGeom should handle geometry without SRID") {
        val wkt = "POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"
        val geom = JTS.fromWKT(wkt)
        
        // Pass null for geomSR - should use raster's SR
        val (resultDs, _) = ClipToGeom.clip(ds, Map.empty, geom, null, cutlineAllTouched = true)

        resultDs should not be null
        resultDs.GetRasterXSize should be > 0
        resultDs.GetRasterYSize should be > 0

        resultDs.delete()
    }

}

