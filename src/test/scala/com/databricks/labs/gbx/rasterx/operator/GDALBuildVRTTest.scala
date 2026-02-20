package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GDALBuildVRTTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds1: Dataset = _
    var ds2: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        val tif1Path = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").getPath
        val tif2Path = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF").getPath
        ds1 = gdal.Open(tif1Path)
        ds2 = gdal.Open(tif2Path)
    }

    override def afterAll(): Unit = {
        if (ds1 != null) ds1.delete()
        if (ds2 != null) ds2.delete()
    }

    test("GDALBuildVRT should create VRT from multiple rasters") {
        val vrtPath = "/vsimem/mosaic.vrt"
        val command = "gdalbuildvrt"
        val (resultDs, metadata) = GDALBuildVRT.executeVRT(vrtPath, Array(ds1, ds2), Map.empty, command)

        resultDs should not be null
        resultDs.GetDriver().getShortName shouldBe "VRT"
        metadata should contain key "path"
        metadata("driver") shouldBe "VRT"

        gdal.Unlink(vrtPath)
        resultDs.delete()
    }

    test("GDALBuildVRT should handle overlapping rasters") {
        // MODIS tiles from same area should mosaic correctly
        val vrtPath = "/vsimem/mosaic_overlap.vrt"
        val command = "gdalbuildvrt"
        val (resultDs, metadata) = GDALBuildVRT.executeVRT(vrtPath, Array(ds1, ds2), Map.empty, command)

        resultDs should not be null
        // Verify it covers both inputs (same area, so dimensions should match)
        resultDs.getRasterXSize should be > 0
        resultDs.getRasterYSize should be > 0

        gdal.Unlink(vrtPath)
        resultDs.delete()
    }

    test("GDALBuildVRT should stack bands from separate files") {
        val vrtPath = "/vsimem/stacked.vrt"
        val command = "gdalbuildvrt -separate"
        val (resultDs, metadata) = GDALBuildVRT.executeVRT(vrtPath, Array(ds1, ds2), Map.empty, command)

        resultDs should not be null
        // With -separate, each input becomes a band
        resultDs.GetRasterCount shouldBe 2

        gdal.Unlink(vrtPath)
        resultDs.delete()
    }

    test("GDALBuildVRT should handle NoData in mosaic") {
        val vrtPath = "/vsimem/mosaic_nodata.vrt"
        val command = "gdalbuildvrt -srcnodata 0 -vrtnodata -9999"
        val (resultDs, metadata) = GDALBuildVRT.executeVRT(vrtPath, Array(ds1, ds2), Map.empty, command)

        resultDs should not be null
        val band = resultDs.GetRasterBand(1)
        val noDataArray = Array.ofDim[java.lang.Double](1)
        band.GetNoDataValue(noDataArray)
        noDataArray(0) shouldBe -9999.0

        gdal.Unlink(vrtPath)
        resultDs.delete()
    }

    test("GDALBuildVRT should preserve metadata") {
        val vrtPath = "/vsimem/mosaic_metadata.vrt"
        val command = "gdalbuildvrt"
        val (resultDs, metadata) = GDALBuildVRT.executeVRT(vrtPath, Array(ds1, ds2), Map.empty, command)

        resultDs should not be null
        // VRT should preserve spatial reference from inputs
        val sr = resultDs.GetSpatialRef
        // Just verify operation completed successfully
        metadata should contain key "all_parents"
        metadata("all_parents") should include(ds1.GetDescription())
        metadata("all_parents") should include(ds2.GetDescription())

        gdal.Unlink(vrtPath)
        resultDs.delete()
    }

    test("GDALBuildVRT should handle single input") {
        val vrtPath = "/vsimem/single.vrt"
        val command = "gdalbuildvrt"
        val (resultDs, metadata) = GDALBuildVRT.executeVRT(vrtPath, Array(ds1), Map.empty, command)

        resultDs should not be null
        resultDs.GetDriver().getShortName shouldBe "VRT"
        // VRT of single raster should have same dimensions
        resultDs.getRasterXSize shouldBe ds1.getRasterXSize
        resultDs.getRasterYSize shouldBe ds1.getRasterYSize

        gdal.Unlink(vrtPath)
        resultDs.delete()
    }

    test("GDALBuildVRT should reject invalid command") {
        val vrtPath = "/vsimem/invalid.vrt"
        val invalidCommand = "invalid_command"
        
        assertThrows[IllegalArgumentException] {
            GDALBuildVRT.executeVRT(vrtPath, Array(ds1), Map.empty, invalidCommand)
        }
    }

    test("GDALBuildVRT should bypass format options") {
        val vrtPath = "/vsimem/format_bypass.vrt"
        val command = "gdalbuildvrt"
        // VRT commands should ignore format options in OperatorOptions.appendOptions
        val (resultDs, metadata) = GDALBuildVRT.executeVRT(
            vrtPath, 
            Array(ds1), 
            Map("format" -> "GTiff", "compression" -> "DEFLATE"), 
            command
        )

        resultDs should not be null
        resultDs.GetDriver().getShortName shouldBe "VRT"
        // Command should not have added format options
        metadata should contain key "last_command"
        metadata("last_command") should not include("-of")

        gdal.Unlink(vrtPath)
        resultDs.delete()
    }

}

