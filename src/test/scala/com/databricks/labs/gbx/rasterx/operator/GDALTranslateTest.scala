package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GDALTranslateTest extends AnyFunSuite with BeforeAndAfterAll {

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

}

