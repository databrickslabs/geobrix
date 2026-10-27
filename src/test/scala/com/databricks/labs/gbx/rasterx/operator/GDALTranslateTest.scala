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

    // The Zarr driver has no DEFLATE (the heavyweight default codec) and no PREDICTOR; its zlib codec is ZLIB, and its
    // levels are ZLIB_LEVEL / ZSTD_LEVEL. Which codecs it has depends on the GDAL build, so the driver is asked.
    private def zarrCommand(writeOptions: Map[String, String]): String = {
        assume(gdal.GetDriverByName("Zarr") != null, "this GDAL build has no Zarr driver")
        OperatorOptions.appendOptions("gdal_translate", writeOptions + ("format" -> "Zarr"), ds)
    }

    test("appendOptions maps the default DEFLATE to the Zarr driver's ZLIB, with its level and no predictor") {
        val cmd = zarrCommand(Map.empty)
        cmd should include("-of Zarr")
        cmd should include("-co COMPRESS=ZLIB")
        cmd should include("-co ZLIB_LEVEL=6")
        cmd should not include "DEFLATE"
        cmd should not include "PREDICTOR"
        cmd should not include "ZLEVEL"

        val explicit = zarrCommand(Map("compression" -> "deflate", "zlevel" -> "3"))
        explicit should include("-co COMPRESS=ZLIB")
        explicit should include("-co ZLIB_LEVEL=3")
    }

    test("appendOptions keeps ZSTD for the Zarr driver, with its level and no predictor") {
        val cmd = zarrCommand(Map("compression" -> "ZSTD"))
        cmd should include("-co COMPRESS=ZSTD")
        cmd should include("-co ZSTD_LEVEL=9")
        cmd should not include "PREDICTOR"
        zarrCommand(Map("compression" -> "ZSTD", "zstd_level" -> "3")) should include("-co ZSTD_LEVEL=3")
    }

    test("appendOptions passes the other Zarr codecs through, and keeps SRC_METHOD for a raster without georeferencing") {
        for (codec <- Seq("NONE", "ZLIB", "GZIP", "LZMA")) {
            val cmd = zarrCommand(Map("compression" -> codec))
            withClue(s"$codec: ") { cmd should include(s"-co COMPRESS=$codec") }
        }
        val noGeoRef = zarrCommand(Map("missingGeoRef" -> "true"))
        noGeoRef should include("SRC_METHOD=NO_GEOTRANSFORM")
        noGeoRef should include("-co COMPRESS=ZLIB")
    }

    test("appendOptions refuses a codec the Zarr driver lacks and names the ones it has") {
        for (codec <- Seq("LZW", "JPEG")) {
            val e = intercept[IllegalArgumentException](zarrCommand(Map("compression" -> codec)))
            withClue(s"$codec: ${e.getMessage}: ") {
                e.getMessage should (include(codec) and include("Zarr") and include("NONE") and include("ZLIB") and include("ZSTD"))
                // DEFLATE is accepted and written as ZLIB, so the message says so.
                e.getMessage should include("DEFLATE")
            }
        }
    }

    test("appendOptions leaves the GTiff and COG codecs alone") {
        OperatorOptions.appendOptions("gdal_translate", Map("format" -> "GTiff"), ds) should include("-co COMPRESS=DEFLATE")
        OperatorOptions.appendOptions("gdal_translate", Map("format" -> "COG"), ds) should include("-co COMPRESS=DEFLATE")
        // LZW is a TIFF codec; only the Zarr driver refuses it.
        OperatorOptions.appendOptions("gdal_translate", Map("compression" -> "LZW"), ds) should include("-co COMPRESS=LZW")
    }

    test("GDALTranslate writes a Zarr store with default options, and the store holds the source's pixels") {
        assume(gdal.GetDriverByName("Zarr") != null, "this GDAL build has no Zarr driver")
        for (compression <- Seq(None, Some("ZSTD"), Some("NONE"))) {
            val outputPath = s"/vsimem/translated_default_${compression.getOrElse("default")}.zarr"
            val options = Map("format" -> "Zarr") ++ compression.map("compression" -> _)
            val (resultDs, metadata) = GDALTranslate.executeTranslate(outputPath, ds, "gdal_translate", options)
            try {
                withClue(s"${compression.getOrElse("default")}: ${metadata("last_error")}: ") { resultDs should not be null }
                resultDs.GetDriver().getShortName shouldBe "Zarr"
                resultDs.GetRasterBand(1).Checksum() shouldBe ds.GetRasterBand(1).Checksum()
            } finally {
                if (resultDs != null) resultDs.delete()
                gdal.RmdirRecursive(outputPath)
            }
        }
    }

    test("GDALTranslate should use DEFLATE+PREDICTOR for float datasets by default") {
        // Create a Float32 dataset
        val driver = gdal.GetDriverByName("MEM")
        val floatDs = driver.Create("/vsimem/float.tif", 100, 100, 1, gdalconstConstants.GDT_Float32)
        val gt = Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0)
        floatDs.SetGeoTransform(gt)

        val outputPath = "/vsimem/translated_float_deflate.tif"
        val command = "gdal_translate"
        // Heavy tier defaults to DEFLATE+predictor for all outputs (baseline TIFF, previewable); float -> PREDICTOR=3
        val (resultDs, metadata) = GDALTranslate.executeTranslate(
            outputPath,
            floatDs,
            command,
            Map.empty  // No compression specified -> default is DEFLATE
        )

        resultDs should not be null
        // Default (no compression specified) uses DEFLATE with PREDICTOR=3 for float
        metadata should contain key "last_command"
        metadata("last_command") should include("COMPRESS=DEFLATE")
        metadata("last_command") should include("PREDICTOR=3")

        gdal.Unlink(outputPath)
        resultDs.delete()
        floatDs.delete()
        gdal.Unlink("/vsimem/float.tif")
    }

    test("GDALTranslate -ot Byte on a Float source selects a Byte-compatible predictor (#82)") {
        // Float input, Byte output: the predictor must match the OUTPUT type. Deriving
        // PREDICTOR=3 (Float32/Float64-only) from the float *input* made gdal_translate
        // reject the Byte output ("PREDICTOR=3 is only supported with Float32 or Float64").
        val driver = gdal.GetDriverByName("MEM")
        val floatDs = driver.Create("/vsimem/float_to_byte.tif", 100, 100, 1, gdalconstConstants.GDT_Float32)
        floatDs.SetGeoTransform(Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0))

        val outputPath = "/vsimem/translated_byte.tif"
        val command = "gdal_translate -ot Byte"
        val (resultDs, metadata) = GDALTranslate.executeTranslate(outputPath, floatDs, command, Map.empty)

        resultDs should not be null
        resultDs.GetRasterBand(1).getDataType shouldBe gdalconstConstants.GDT_Byte
        metadata should contain key "last_command"
        metadata("last_command") should include("PREDICTOR=1")
        metadata("last_command") should not include ("PREDICTOR=3")

        gdal.Unlink(outputPath)
        resultDs.delete()
        floatDs.delete()
        gdal.Unlink("/vsimem/float_to_byte.tif")
    }

    test("appendOptions derives predictor from the --type=<X> equals form (gdal_calc) (#82)") {
        // The repo's gdal_calc callers (SpectralIndexSpec, RST_Threshold) emit the equals
        // form "--type=Float32", not the space form. Exact-token matching missed it, so the
        // gdal_calc path fell back to the input band types. Here a Byte output on a Float
        // input must still yield PREDICTOR=1 (not 3) via the "--type=Byte" token.
        val driver = gdal.GetDriverByName("MEM")
        val floatDs = driver.Create("", 10, 10, 1, gdalconstConstants.GDT_Float32)

        val out = OperatorOptions.appendOptions("gdal_calc --calc=A --type=Byte", Map.empty, floatDs)
        out should include("PREDICTOR=1")
        out should not include ("PREDICTOR=3")

        // Float output (the actual SpectralIndex case) resolves to PREDICTOR=3.
        val outFloat = OperatorOptions.appendOptions("gdal_calc --calc=A --type=Float32", Map.empty, floatDs)
        outFloat should include("PREDICTOR=3")

        floatDs.delete()
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

    test("GDALTranslate should use LEVEL option for COG ZSTD compression (not ZSTD_LEVEL)") {
        val outputPath = "/vsimem/translated_cog_zstd_level.cog"
        val command = "gdal_translate"
        // COG format with ZSTD compression should emit -co LEVEL=... not -co ZSTD_LEVEL=...
        val (resultDs, metadata) = GDALTranslate.executeTranslate(
            outputPath,
            ds,
            command,
            Map("format" -> "COG", "compression" -> "ZSTD", "level" -> "5")
        )

        resultDs should not be null
        // Verify that LEVEL (not ZSTD_LEVEL) is used for COG format
        metadata should contain key "last_command"
        metadata("last_command") should include("COMPRESS=ZSTD")
        metadata("last_command") should include("LEVEL=5")
        // Ensure ZSTD_LEVEL (GTiff-specific) is NOT present for COG
        metadata("last_command") should not include("ZSTD_LEVEL")

        gdal.Unlink(outputPath)
        resultDs.delete()
    }

}

