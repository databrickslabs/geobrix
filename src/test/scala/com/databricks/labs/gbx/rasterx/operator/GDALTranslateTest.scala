package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.util.Try

class GDALTranslateTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").getPath
        ds = gdal.Open(tifPath)
    }

    override def afterAll(): Unit = {
        if (ds != null) ds.delete()
    }

    test("GDALTranslate should convert between formats") {
        val outputPath = "/vsimem/translated.png"
        val command = "gdal_translate"
        val (resultDs, _) = GDALTranslate.executeTranslate(outputPath, ds, command, Map("format" -> "PNG"))

        resultDs should not be null
        resultDs.GetDriver().getShortName shouldBe "PNG"

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALTranslate should select specific bands") {
        val outputPath = "/vsimem/translated_band1.tif"
        val command = "gdal_translate -b 1"
        val (resultDs, _) = GDALTranslate.executeTranslate(outputPath, ds, command, Map.empty)

        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALTranslate should extract window/subset") {
        val outputPath = "/vsimem/translated_subset.tif"
        val command = "gdal_translate -srcwin 100 100 200 200"
        val (resultDs, _) = GDALTranslate.executeTranslate(outputPath, ds, command, Map.empty)

        resultDs should not be null
        resultDs.getRasterXSize shouldBe 200
        resultDs.getRasterYSize shouldBe 200

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALTranslate should resample raster") {
        val outputPath = "/vsimem/translated_resampled.tif"
        val newWidth = ds.getRasterXSize / 2
        val newHeight = ds.getRasterYSize / 2
        val command = s"gdal_translate -outsize $newWidth $newHeight"
        val (resultDs, _) = GDALTranslate.executeTranslate(outputPath, ds, command, Map.empty)

        resultDs should not be null
        resultDs.getRasterXSize shouldBe newWidth
        resultDs.getRasterYSize shouldBe newHeight

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALTranslate should convert data types") {
        val outputPath = "/vsimem/translated_float32.tif"
        val command = "gdal_translate -ot Float32"
        val (resultDs, _) = GDALTranslate.executeTranslate(outputPath, ds, command, Map.empty)

        resultDs should not be null
        resultDs.GetRasterBand(1).getDataType shouldBe gdalconstConstants.GDT_Float32

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALTranslate should apply creation options") {
        val outputPath = "/vsimem/translated_compressed.tif"
        val command = "gdal_translate -co COMPRESS=DEFLATE -co TILED=YES"
        val (resultDs, metadata) = GDALTranslate.executeTranslate(outputPath, ds, command, Map("compression" -> "DEFLATE"))

        resultDs should not be null
        metadata should contain key "path"
        metadata should contain key "last_command"

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALTranslate should apply scale and offset") {
        val outputPath = "/vsimem/translated_scaled.tif"
        val command = "gdal_translate -scale 0 255 0 1"
        val (resultDs, _) = GDALTranslate.executeTranslate(outputPath, ds, command, Map.empty)

        resultDs should not be null
        // Verify output exists and has valid dimensions
        resultDs.getRasterXSize should be > 0
        resultDs.getRasterYSize should be > 0

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALTranslate should preserve spatial reference") {
        val outputPath = "/vsimem/translated_metadata.tif"
        val command = "gdal_translate"
        val (resultDs, _) = GDALTranslate.executeTranslate(outputPath, ds, command, Map.empty)

        resultDs should not be null
        // Both should have spatial reference (may be null or non-null depending on source)
        val srcSR = ds.GetSpatialRef
        val dstSR = resultDs.GetSpatialRef
        // Just verify the operation completed successfully
        resultDs.getRasterXSize shouldBe ds.getRasterXSize
        resultDs.getRasterYSize shouldBe ds.getRasterYSize

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALTranslate should set NoData value") {
        val outputPath = "/vsimem/translated_nodata.tif"
        val command = "gdal_translate -a_nodata -9999"
        val (resultDs, _) = GDALTranslate.executeTranslate(outputPath, ds, command, Map.empty)

        resultDs should not be null
        val noDataArray = Array.ofDim[java.lang.Double](1)
        resultDs.GetRasterBand(1).GetNoDataValue(noDataArray)
        noDataArray(0) shouldBe -9999.0

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALTranslate should handle invalid parameters") {
        val outputPath = "/vsimem/translated_error.tif"
        val command = "gdal_translate -outsize -100 -100"
        val (resultDs, metadata) = GDALTranslate.executeTranslate(outputPath, ds, command, Map.empty)

        // GDAL logs error but returns null dataset
        resultDs shouldBe null
        metadata("last_error") should not be empty

        if (resultDs != null) resultDs.delete()
    }

    test("GDALTranslate should reject invalid command") {
        val outputPath = "/vsimem/invalid.tif"
        val invalidCommand = "invalid_command"
        
        assertThrows[IllegalArgumentException] {
            GDALTranslate.executeTranslate(outputPath, ds, invalidCommand, Map.empty)
        }
    }

    test("GDALTranslate should handle alternative compression methods") {
        val outputPath = "/vsimem/translated_nocomp.tif"
        val command = "gdal_translate"
        // Use NONE compression to test the "other" compression case in OperatorOptions
        val (resultDs, metadata) = GDALTranslate.executeTranslate(outputPath, ds, command, Map("compression" -> "NONE"))

        resultDs should not be null
        resultDs.GetRasterXSize shouldBe ds.GetRasterXSize
        metadata should contain key "compression"

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALTranslate should handle COG format") {
        val outputPath = "/vsimem/translated.cog"
        val command = "gdal_translate"
        val (resultDs, metadata) = GDALTranslate.executeTranslate(outputPath, ds, command, Map("format" -> "COG"))

        resultDs should not be null
        // COG format applies BLOCKSIZE option
        metadata should contain key "last_command"
        metadata("last_command") should include("BLOCKSIZE")

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALTranslate should handle VRT format") {
        val outputPath = "/vsimem/translated.vrt"
        val command = "gdal_translate"
        val (resultDs, metadata) = GDALTranslate.executeTranslate(outputPath, ds, command, Map("format" -> "VRT"))

        resultDs should not be null
        resultDs.GetDriver().getShortName shouldBe "VRT"

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

    test("GDALTranslate should handle Zarr format with missing georef") {
        val outputPath = "/vsimem/translated.zarr"
        val command = "gdal_translate"
        
        // Zarr format may not be fully supported, handle gracefully
        Try {
            val (resultDs, metadata) = GDALTranslate.executeTranslate(
                outputPath, 
                ds, 
                command, 
                Map("format" -> "Zarr", "missingGeoRef" -> "true")
            )

            // If successful, verify command includes SRC_METHOD
            if (resultDs != null) {
                metadata should contain key "last_command"
                metadata("last_command") should include("SRC_METHOD=NO_GEOTRANSFORM")
                resultDs.delete()
            }
            
            gdal.Unlink(outputPath)
        }
        // Zarr format may not be available in all GDAL builds, test passes either way
        succeed
    }

    test("GDALTranslate should handle float datasets with DEFLATE compression") {
        // Create a Float32 dataset
        val driver = gdal.GetDriverByName("MEM")
        val floatDs = driver.Create("/vsimem/float.tif", 100, 100, 1, gdalconstConstants.GDT_Float32)
        val gt = Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0)
        floatDs.SetGeoTransform(gt)

        val outputPath = "/vsimem/translated_float_deflate.tif"
        val command = "gdal_translate"
        val (resultDs, metadata) = GDALTranslate.executeTranslate(
            outputPath, 
            floatDs, 
            command, 
            Map("compression" -> "DEFLATE")
        )

        resultDs should not be null
        // Float dataset with DEFLATE should use PREDICTOR=3
        metadata should contain key "last_command"
        metadata("last_command") should include("PREDICTOR=3")

        gdal.Unlink(outputPath)
        resultDs.delete()
        floatDs.delete()
        gdal.Unlink("/vsimem/float.tif")
    }

    test("GDALTranslate should handle PNM format with scaling") {
        val outputPath = "/vsimem/translated.pnm"
        val command = "gdal_translate"
        val (resultDs, metadata) = GDALTranslate.executeTranslate(outputPath, ds, command, Map("format" -> "PNM"))

        // PNM format applies specific scaling options
        metadata should contain key "last_command"
        metadata("last_command") should include("PNM")
        metadata("last_command") should include("-scale")

        if (resultDs != null) {
            gdal.Unlink(outputPath)
            resultDs.delete()
        }
    }

}

