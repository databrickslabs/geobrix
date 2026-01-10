package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class RasterProjectTest extends AnyFunSuite with BeforeAndAfterAll {

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

    test("RasterProject should project raster to different CRS") {
        val dstSR = new SpatialReference()
        dstSR.ImportFromEPSG(4326) // WGS84

        val (resultDs, metadata) = RasterProject.project(ds, Map.empty, dstSR)

        resultDs should not be null
        resultDs.GetRasterCount shouldBe ds.GetRasterCount

        metadata should not be null
        metadata should contain key "path"

        // Verify the projection changed
        val resultSR = resultDs.GetSpatialRef
        resultSR should not be null
        val resultEPSG = resultSR.GetAuthorityCode(null)
        resultEPSG shouldBe "4326"

        resultDs.delete()
        dstSR.delete()
    }

    test("RasterProject should preserve band count") {
        val dstSR = new SpatialReference()
        dstSR.ImportFromEPSG(4326)

        val originalBandCount = ds.GetRasterCount

        val (resultDs, _) = RasterProject.project(ds, Map.empty, dstSR)

        resultDs.GetRasterCount shouldBe originalBandCount

        resultDs.delete()
        dstSR.delete()
    }

    test("RasterProject should handle projection to UTM") {
        val dstSR = new SpatialReference()
        dstSR.ImportFromEPSG(32611) // UTM Zone 11N

        val (resultDs, metadata) = RasterProject.project(ds, Map.empty, dstSR)

        resultDs should not be null
        
        val resultSR = resultDs.GetSpatialRef
        val resultEPSG = resultSR.GetAuthorityCode(null)
        resultEPSG shouldBe "32611"

        resultDs.delete()
        dstSR.delete()
    }

    test("RasterProject should handle projection to same CRS") {
        // Use a known CRS (WGS84) for this test
        val dstSR = new SpatialReference()
        dstSR.ImportFromEPSG(4326)

        // Project to WGS84 first
        val (intermediateDs, _) = RasterProject.project(ds, Map.empty, dstSR)
        
        // Now project again to the same CRS
        val dstSR2 = new SpatialReference()
        dstSR2.ImportFromEPSG(4326)
        
        val (resultDs, _) = RasterProject.project(intermediateDs, Map.empty, dstSR2)

        resultDs should not be null
        
        // Should still produce valid result even if same CRS
        val resultSR = resultDs.GetSpatialRef
        val resultEPSG = resultSR.GetAuthorityCode(null)
        resultEPSG shouldBe "4326"

        resultDs.delete()
        intermediateDs.delete()
        dstSR.delete()
        dstSR2.delete()
    }

    test("RasterProject should produce valid geotransform") {
        val dstSR = new SpatialReference()
        dstSR.ImportFromEPSG(4326)

        val (resultDs, _) = RasterProject.project(ds, Map.empty, dstSR)

        val resultGT = resultDs.GetGeoTransform()
        
        resultGT should not be null
        resultGT.length shouldBe 6
        
        // All geotransform values should be valid numbers
        resultGT.foreach { value =>
            value.isNaN shouldBe false
            value.isInfinite shouldBe false
        }

        resultDs.delete()
        dstSR.delete()
    }

    test("RasterProject should preserve raster data") {
        val dstSR = new SpatialReference()
        dstSR.ImportFromEPSG(4326)

        val (resultDs, _) = RasterProject.project(ds, Map.empty, dstSR)

        val resultBand = resultDs.GetRasterBand(1)
        val stats = resultBand.AsMDArray().GetStatistics()
        
        stats should not be null
        stats.getMin should be >= 0.0
        stats.getMax should be > 0.0
        stats.getMax should be > stats.getMin

        resultDs.delete()
        dstSR.delete()
    }

    test("RasterProject should handle Web Mercator projection") {
        val dstSR = new SpatialReference()
        dstSR.ImportFromEPSG(3857) // Web Mercator

        val (resultDs, metadata) = RasterProject.project(ds, Map.empty, dstSR)

        resultDs should not be null
        
        val resultSR = resultDs.GetSpatialRef
        val resultEPSG = resultSR.GetAuthorityCode(null)
        resultEPSG shouldBe "3857"

        resultDs.delete()
        dstSR.delete()
    }

    test("RasterProject should maintain reasonable dimensions") {
        val dstSR = new SpatialReference()
        dstSR.ImportFromEPSG(4326)

        val originalXSize = ds.GetRasterXSize
        val originalYSize = ds.GetRasterYSize

        val (resultDs, _) = RasterProject.project(ds, Map.empty, dstSR)

        // Dimensions should be reasonable (within 10x of original)
        resultDs.GetRasterXSize should be > 0
        resultDs.GetRasterYSize should be > 0
        resultDs.GetRasterXSize should be < (originalXSize * 10)
        resultDs.GetRasterYSize should be < (originalYSize * 10)

        resultDs.delete()
        dstSR.delete()
    }

    test("RasterProject should preserve NoData values") {
        val originalBand = ds.GetRasterBand(1)
        val originalNoData = Array.fill[java.lang.Double](1)(0)
        originalBand.GetNoDataValue(originalNoData)

        val dstSR = new SpatialReference()
        dstSR.ImportFromEPSG(4326)

        val (resultDs, _) = RasterProject.project(ds, Map.empty, dstSR)

        val resultBand = resultDs.GetRasterBand(1)
        val resultNoData = Array.fill[java.lang.Double](1)(0)
        resultBand.GetNoDataValue(resultNoData)

        if (originalNoData(0) != null) {
            resultNoData(0) should not be null
            Math.abs(resultNoData(0) - originalNoData(0)) should be < 0.0001
        }

        resultDs.delete()
        dstSR.delete()
    }

    test("RasterProject should handle options parameter") {
        val dstSR = new SpatialReference()
        dstSR.ImportFromEPSG(4326)

        val options = Map("compression" -> "DEFLATE")

        val (resultDs, metadata) = RasterProject.project(ds, options, dstSR)

        resultDs should not be null
        metadata should not be null

        resultDs.delete()
        dstSR.delete()
    }

}

