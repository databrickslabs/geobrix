package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GDALWarpTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _

    override def beforeAll(): Unit = {
        super.beforeAll()
        GDALManager.configureGDAL("/tmp", "/tmp")
        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").getPath
        ds = gdal.Open(tifPath)
    }

    override def afterAll(): Unit = {
        if (ds != null) ds.delete()
    }

    test("GDALWarp should transform raster to different CRS") {
        val outputPath = "/vsimem/warped_test.tif"
        val command = "gdalwarp -t_srs EPSG:4326"
        val (resultDs, metadata) = GDALWarp.executeWarp(outputPath, Array(ds), Map.empty, command)

        resultDs should not be null
        val resultSR = resultDs.GetSpatialRef
        resultSR should not be null

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALWarp should support different resampling methods") {
        val methods = Seq("near", "bilinear", "cubic")

        methods.foreach { method =>
            val outputPath = s"/vsimem/warped_$method.tif"
            val command = s"gdalwarp -r $method -t_srs EPSG:4326"
            val (resultDs, _) = GDALWarp.executeWarp(outputPath, Array(ds), Map.empty, command)

            resultDs should not be null
            resultDs.GetRasterCount shouldBe ds.GetRasterCount

            gdal.Unlink(outputPath)
            resultDs.delete()
        }
    }

    test("GDALWarp should crop raster to specified bounds") {
        val outputPath = "/vsimem/warped_cropped.tif"
        // Use valid bounds within the MODIS sinusoidal projection
        val command = "gdalwarp -te -8900000 2200000 -8880000 2220000"
        val (resultDs, _) = GDALWarp.executeWarp(outputPath, Array(ds), Map.empty, command)

        resultDs should not be null
        resultDs.getRasterXSize should be < ds.getRasterXSize
        resultDs.getRasterYSize should be < ds.getRasterYSize

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALWarp should change raster resolution") {
        val outputPath = "/vsimem/warped_resampled.tif"
        val command = "gdalwarp -tr 1000 1000"
        val (resultDs, _) = GDALWarp.executeWarp(outputPath, Array(ds), Map.empty, command)

        resultDs should not be null
        val gt = Array.ofDim[Double](6)
        resultDs.GetGeoTransform(gt)
        math.abs(gt(1)) shouldBe 1000.0 +- 10.0  // X pixel size
        math.abs(gt(5)) shouldBe 1000.0 +- 10.0  // Y pixel size (negative)

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALWarp should mosaic multiple rasters") {
        val ds2Path = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF").getPath
        val ds2 = gdal.Open(ds2Path)

        val outputPath = "/vsimem/warped_mosaic.tif"
        val command = "gdalwarp"
        val (resultDs, _) = GDALWarp.executeWarp(outputPath, Array(ds, ds2), Map.empty, command)

        resultDs should not be null
        resultDs.GetRasterCount should be >= 1

        gdal.Unlink(outputPath)
        resultDs.delete()
        ds2.delete()
    }

    test("GDALWarp should handle NoData values correctly") {
        val outputPath = "/vsimem/warped_nodata.tif"
        val command = "gdalwarp -dstnodata -9999"
        val (resultDs, _) = GDALWarp.executeWarp(outputPath, Array(ds), Map.empty, command)

        resultDs should not be null
        val band = resultDs.GetRasterBand(1)
        val noDataArray = Array.ofDim[java.lang.Double](1)
        band.GetNoDataValue(noDataArray)
        noDataArray(0) shouldBe -9999.0

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALWarp should respect creation options") {
        val outputPath = "/vsimem/warped_compressed.tif"
        val command = "gdalwarp -co COMPRESS=LZW -co TILED=YES"
        val (resultDs, metadata) = GDALWarp.executeWarp(outputPath, Array(ds), Map("compression" -> "LZW"), command)

        resultDs should not be null
        metadata should contain key "path"
        metadata should contain key "last_command"

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALWarp should handle errors gracefully") {
        val caught = intercept[Exception] {
            val outputPath = "/vsimem/warped_error.tif"
            val command = "gdalwarp -t_srs INVALID_CRS"
            GDALWarp.executeWarp(outputPath, Array(ds), Map.empty, command)
        }
        // Just verify an exception was thrown
        caught should not be null
    }

}

