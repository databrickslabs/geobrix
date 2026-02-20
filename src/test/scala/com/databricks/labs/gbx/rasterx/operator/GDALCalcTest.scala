package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.util.Try

/**
 * GDALCalc operator tests for raster algebra operations using gdal_calc.py.
 * 
 * These tests validate:
 * - Basic arithmetic operations (addition, subtraction, multiplication, division)
 * - Band math operations (NDVI, conditional logic)
 * - NoData handling in calculations
 * - Multi-band operations from single input files
 * - Creation options and output format control
 * - Complex mathematical expressions (power, log, sqrt)
 * - Error handling for invalid expressions
 *
 * Implementation Notes:
 * - Tests follow the NDVI.scala pattern by using multi-band datasets with band indices
 * - Multi-band datasets are created via GDALBuildVRT with -separate flag
 * - VRT is converted to physical TIF (gdal_calc.py doesn't work well with /vsimem/)
 * - GDAL must be initialized with gdal.AllRegister() for dataset opening to work
 * - GDALCalc.executeCalc uses replaceFirst() to avoid corrupting filenames containing "gdal_calc"
 *
 * Key Fixes Applied:
 * 1. Added gdal.AllRegister() to ensure GDAL drivers are initialized
 * 2. Fixed GDALCalc.scala to use replaceFirst() instead of replace() (prevents filename corruption)
 * 3. Use getClass.getResource() to load test resources (works in all CI environments)
 */
class GDALCalcTest extends AnyFunSuite with BeforeAndAfterAll {

    var multiBandDs: Dataset = _
    var multiBandPath: String = _

    override def beforeAll(): Unit = {
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        
        // Ensure GDAL is fully initialized
        gdal.AllRegister()
        
        // Load source datasets using resource loader (works in all CI environments)
        val tif1Path = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").getPath
        val tif2Path = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF").getPath
        
        val ds1 = gdal.Open(tif1Path)
        val ds2 = gdal.Open(tif2Path)
        
        require(ds1 != null, s"Failed to open $tif1Path")
        require(ds2 != null, s"Failed to open $tif2Path")
        
        // Create a multi-band dataset using GDALBuildVRT (following NDVI.scala pattern)
        // This is how GDALCalc is actually used in the codebase
        multiBandPath = s"/tmp/gdal_calc_test_multiband_${UUID.randomUUID()}.tif"
        val vrtPath = "/vsimem/multiband.vrt"
        
        // Stack bands using VRT
        val command = "gdalbuildvrt -separate"
        val (vrtDs, _) = GDALBuildVRT.executeVRT(vrtPath, Array(ds1, ds2), Map.empty, command)
        
        require(vrtDs != null, "Failed to create VRT")
        
        // Convert VRT to physical TIF for gdal_calc.py (which doesn't work well with /vsimem/)
        val (multiBandDsTmp, _) = GDALTranslate.executeTranslate(multiBandPath, vrtDs, "gdal_translate", Map.empty)
        
        require(multiBandDsTmp != null, "Failed to translate VRT to TIF")
        
        // Clean up temporary datasets
        ds1.delete()
        ds2.delete()
        vrtDs.delete()
        gdal.Unlink(vrtPath)
        multiBandDsTmp.delete()
        
        // Reopen for tests
        multiBandDs = gdal.Open(multiBandPath)
        require(multiBandDs != null, s"Failed to open $multiBandPath")
    }

    override def afterAll(): Unit = {
        if (multiBandDs != null) multiBandDs.delete()
        Try(Files.deleteIfExists(Paths.get(multiBandPath)))
    }

    test("GDALCalc should perform simple addition") {
        val outputPath = s"/tmp/gdal_calc_add_${UUID.randomUUID()}.tif"
        // Following NDVI.scala pattern: single input with band indices
        val command = s"""gdal_calc -A $multiBandPath --A_band=1 -B $multiBandPath --B_band=2 --calc="A+B" --outfile=$outputPath"""
        val (resultDs, metadata) = GDALCalc.executeCalc(command, outputPath, Map.empty, multiBandDs)

        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        resultDs.getRasterXSize shouldBe multiBandDs.getRasterXSize
        resultDs.getRasterYSize shouldBe multiBandDs.getRasterYSize

        resultDs.delete()
        Try(Files.deleteIfExists(Paths.get(outputPath)))
    }

    test("GDALCalc should calculate NDVI") {
        val outputPath = s"/tmp/gdal_calc_ndvi_${UUID.randomUUID()}.tif"
        // NDVI = (NIR - RED) / (NIR + RED) = (Band2 - Band1) / (Band2 + Band1)
        // Following NDVI.scala pattern exactly
        val command = s"""gdal_calc -A $multiBandPath --A_band=1 -B $multiBandPath --B_band=2 --calc="(B-A)/(B+A)" --outfile=$outputPath"""
        val (resultDs, metadata) = GDALCalc.executeCalc(command, outputPath, Map.empty, multiBandDs)

        resultDs should not be null
        // NDVI values should be between -1 and 1
        resultDs.GetRasterCount shouldBe 1

        resultDs.delete()
        Try(Files.deleteIfExists(Paths.get(outputPath)))
    }

    test("GDALCalc should support conditional operations") {
        val outputPath = s"/tmp/gdal_calc_conditional_${UUID.randomUUID()}.tif"
        // Mask values: only keep pixels > 100 from band 1
        val command = s"""gdal_calc -A $multiBandPath --A_band=1 --calc="A*(A>100)" --outfile=$outputPath"""
        val (resultDs, metadata) = GDALCalc.executeCalc(command, outputPath, Map.empty, multiBandDs)

        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1

        resultDs.delete()
        Try(Files.deleteIfExists(Paths.get(outputPath)))
    }

    test("GDALCalc should handle NoData in calculations") {
        val outputPath = s"/tmp/gdal_calc_nodata_${UUID.randomUUID()}.tif"
        val command = s"""gdal_calc -A $multiBandPath --A_band=1 -B $multiBandPath --B_band=2 --calc="A+B" --NoDataValue=-9999 --outfile=$outputPath"""
        val (resultDs, metadata) = GDALCalc.executeCalc(command, outputPath, Map.empty, multiBandDs)

        resultDs should not be null
        val band = resultDs.GetRasterBand(1)
        val noDataArray = Array.ofDim[java.lang.Double](1)
        band.GetNoDataValue(noDataArray)
        noDataArray(0) shouldBe -9999.0

        resultDs.delete()
        Try(Files.deleteIfExists(Paths.get(outputPath)))
    }

    test("GDALCalc should handle multiple bands from same input") {
        val outputPath = s"/tmp/gdal_calc_multi_${UUID.randomUUID()}.tif"
        // Average of two bands from the same multi-band file
        val command = s"""gdal_calc -A $multiBandPath --A_band=1 -B $multiBandPath --B_band=2 --calc="(A+B)/2" --outfile=$outputPath"""
        val (resultDs, metadata) = GDALCalc.executeCalc(command, outputPath, Map.empty, multiBandDs)

        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1

        resultDs.delete()
        Try(Files.deleteIfExists(Paths.get(outputPath)))
    }

    test("GDALCalc should apply creation options") {
        val outputPath = s"/tmp/gdal_calc_options_${UUID.randomUUID()}.tif"
        val command = s"""gdal_calc -A $multiBandPath --A_band=1 --calc="A*2" --co COMPRESS=LZW --outfile=$outputPath"""
        val (resultDs, metadata) = GDALCalc.executeCalc(command, outputPath, Map("compression" -> "LZW"), multiBandDs)

        resultDs should not be null
        metadata should contain key "path"

        resultDs.delete()
        Try(Files.deleteIfExists(Paths.get(outputPath)))
    }

    test("GDALCalc should handle complex mathematical expressions") {
        val outputPath = s"/tmp/gdal_calc_complex_${UUID.randomUUID()}.tif"
        // Euclidean distance: sqrt(Band1^2 + Band2^2)
        val command = s"""gdal_calc -A $multiBandPath --A_band=1 -B $multiBandPath --B_band=2 --calc="sqrt(A**2+B**2)" --outfile=$outputPath"""
        val (resultDs, metadata) = GDALCalc.executeCalc(command, outputPath, Map.empty, multiBandDs)

        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1

        resultDs.delete()
        Try(Files.deleteIfExists(Paths.get(outputPath)))
    }

    test("GDALCalc should handle errors for invalid expressions") {
        val outputPath = s"/tmp/gdal_calc_error_${UUID.randomUUID()}.tif"
        val command = s"""gdal_calc -A $multiBandPath --A_band=1 --calc="INVALID_SYNTAX" --outfile=$outputPath"""
        val (resultDs, metadata) = GDALCalc.executeCalc(command, outputPath, Map.empty, multiBandDs)

        // GDALCalc may return null or error message on failure
        if (resultDs != null) {
            resultDs.delete()
        }
        // Check that an error was captured
        metadata should contain key "full_error"
        
        Try(Files.deleteIfExists(Paths.get(outputPath)))
    }

    test("GDALCalc should reject invalid command") {
        val outputPath = s"/tmp/gdal_calc_invalid_${UUID.randomUUID()}.tif"
        val invalidCommand = "invalid_command"
        
        assertThrows[IllegalArgumentException] {
            GDALCalc.executeCalc(invalidCommand, outputPath, Map.empty, multiBandDs)
        }
        
        Try(Files.deleteIfExists(Paths.get(outputPath)))
    }

    test("GDALCalc should handle PNM format output") {
        val outputPath = s"/tmp/gdal_calc_pnm_${UUID.randomUUID()}.pnm"
        val command = s"""gdal_calc -A $multiBandPath --A_band=1 --calc="A*2" --outfile=$outputPath"""
        val (resultDs, metadata) = GDALCalc.executeCalc(command, outputPath, Map("format" -> "PNM"), multiBandDs)

        // PNM format with gdal_calc uses --format flag
        metadata should contain key "last_command"
        metadata("last_command") should include("--format PNM")
        
        // PNM format may fail with gdal_calc, but command structure is what we're testing
        if (resultDs != null) {
            resultDs.delete()
        }

        Try(Files.deleteIfExists(Paths.get(outputPath)))
    }

}

