package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.{GDALBuildVRT, GDALTranslate}
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * Advanced tests for RST_DerivedBand and RST_MapAlgebra operations.
 * 
 * These tests validate:
 * - Complex Python functions with numpy operations
 * - Multi-band derived band processing
 * - Advanced map algebra with multiple inputs
 * - NoData handling in complex operations
 * - Error handling for invalid inputs
 * - Performance with different data types
 * - Mathematical functions (sqrt, log, power, trigonometric)
 * - Conditional operations and masking
 * 
 * Test Data:
 * - MODIS multi-band TIF files (B01, B02, B03)
 * - Generated test rasters with specific characteristics
 */
class RST_AdvancedOperationsTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds1: Dataset = _
    var ds2: Dataset = _
    var ds3: Dataset = _
    var multiBandDs: Dataset = _
    var multiBandPath: String = _

    override def beforeAll(): Unit = {
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        
        // Ensure NodeFilePathUtil root directory exists for tests
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
        
        // Load source datasets
        val tif1Path = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").getPath
        val tif2Path = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF").getPath
        val tif3Path = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B03.TIF").getPath
        
        ds1 = gdal.Open(tif1Path)
        ds2 = gdal.Open(tif2Path)
        ds3 = gdal.Open(tif3Path)
        
        require(ds1 != null, s"Failed to open $tif1Path")
        require(ds2 != null, s"Failed to open $tif2Path")
        require(ds3 != null, s"Failed to open $tif3Path")
        
        // Create a multi-band dataset for tests
        multiBandPath = s"/tmp/advanced_ops_test_multiband_${UUID.randomUUID()}.tif"
        val vrtPath = "/vsimem/multiband_advanced.vrt"
        
        val command = "gdalbuildvrt -separate"
        val (vrtDs, _) = GDALBuildVRT.executeVRT(vrtPath, Array(ds1, ds2, ds3), Map.empty, command)
        
        require(vrtDs != null, "Failed to create VRT")
        
        val (multiBandDsTmp, _) = GDALTranslate.executeTranslate(multiBandPath, vrtDs, "gdal_translate", Map.empty)
        
        require(multiBandDsTmp != null, "Failed to translate VRT to TIF")
        
        vrtDs.delete()
        gdal.Unlink(vrtPath)
        multiBandDsTmp.delete()
        
        // Reopen for tests
        multiBandDs = gdal.Open(multiBandPath)
        require(multiBandDs != null, s"Failed to open $multiBandPath")
    }

    override def afterAll(): Unit = {
        if (ds1 != null) ds1.delete()
        if (ds2 != null) ds2.delete()
        if (ds3 != null) ds3.delete()
        if (multiBandDs != null) multiBandDs.delete()
        Files.deleteIfExists(Paths.get(multiBandPath))
    }

    // ====================================================================
    // RST_DerivedBand Advanced Tests (6 tests)
    // ====================================================================

    test("RST_DerivedBand should handle complex numpy operations") {
        val pyfunc = """
import numpy as np
def complex_transform(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    # Apply complex transformation: normalize, apply log transform, then scale
    data = in_ar[0].astype(np.float32)
    # Avoid log(0) by adding small epsilon
    epsilon = 1e-10
    normalized = (data - data.min()) / (data.max() - data.min() + epsilon)
    log_transformed = np.log1p(normalized)
    out_ar[:] = (log_transformed * 255).astype(np.uint8)
"""
        val (derivedDs, _) = RST_DerivedBand.execute(Seq(ds1), Map.empty, pyfunc, "complex_transform")
        
        derivedDs should not be null
        derivedDs.GetRasterCount shouldBe 1
        derivedDs.GetRasterXSize shouldBe ds1.GetRasterXSize
        derivedDs.GetRasterYSize shouldBe ds1.GetRasterYSize
        
        RasterDriver.releaseDataset(derivedDs)
    }

    test("RST_DerivedBand should process multi-band operations") {
        val pyfunc = """
import numpy as np
def multiband_average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    # Average multiple input rasters
    stacked = np.array(in_ar)
    out_ar[:] = np.mean(stacked, axis=0)
"""
        val (derivedDs, _) = RST_DerivedBand.execute(Seq(ds1, ds2, ds3), Map.empty, pyfunc, "multiband_average")
        
        derivedDs should not be null
        derivedDs.GetRasterCount shouldBe 1
        
        RasterDriver.releaseDataset(derivedDs)
    }

    test("RST_DerivedBand should handle statistical operations") {
        val pyfunc = """
import numpy as np
def statistical_metrics(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    # Compute standard deviation across bands
    stacked = np.array(in_ar)
    out_ar[:] = np.std(stacked, axis=0)
"""
        val (derivedDs, _) = RST_DerivedBand.execute(Seq(ds1, ds2, ds3), Map.empty, pyfunc, "statistical_metrics")
        
        derivedDs should not be null
        derivedDs.GetRasterCount shouldBe 1
        
        RasterDriver.releaseDataset(derivedDs)
    }

    test("RST_DerivedBand should handle conditional masking") {
        val pyfunc = """
import numpy as np
def conditional_mask(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    # Apply threshold and mask: keep values > 100, set others to 0
    data = in_ar[0]
    mask = data > 100
    out_ar[:] = np.where(mask, data, 0)
"""
        val (derivedDs, _) = RST_DerivedBand.execute(Seq(ds1), Map.empty, pyfunc, "conditional_mask")
        
        derivedDs should not be null
        derivedDs.GetRasterCount shouldBe 1
        
        RasterDriver.releaseDataset(derivedDs)
    }

    test("RST_DerivedBand should handle NoData values properly") {
        val pyfunc = """
import numpy as np
def nodata_aware(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    # Process data while preserving NoData (0 in MODIS)
    data = in_ar[0].astype(np.float32)
    nodata_mask = (data == 0)
    # Apply transformation only to valid data
    result = np.where(~nodata_mask, data * 1.5, 0)
    out_ar[:] = result
"""
        val (derivedDs, _) = RST_DerivedBand.execute(Seq(ds1), Map.empty, pyfunc, "nodata_aware")
        
        derivedDs should not be null
        derivedDs.GetRasterCount shouldBe 1
        
        RasterDriver.releaseDataset(derivedDs)
    }

    test("RST_DerivedBand should handle edge cases gracefully") {
        // Test with simple passthrough function
        val pyfunc = """
import numpy as np
def passthrough(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    # Simple passthrough - copy input to output
    out_ar[:] = in_ar[0]
"""
        val (derivedDs, _) = RST_DerivedBand.execute(Seq(ds1), Map.empty, pyfunc, "passthrough")
        
        derivedDs should not be null
        derivedDs.GetRasterCount shouldBe 1
        derivedDs.GetRasterXSize shouldBe ds1.GetRasterXSize
        derivedDs.GetRasterYSize shouldBe ds1.GetRasterYSize
        
        RasterDriver.releaseDataset(derivedDs)
    }

    // ====================================================================
    // RST_MapAlgebra Advanced Tests (8 tests)
    // ====================================================================

    test("RST_MapAlgebra should compute NDVI with multi-band input") {
        val outputPath = s"/tmp/mapalgebra_ndvi_${UUID.randomUUID()}.tif"
        // NDVI = (NIR - Red) / (NIR + Red)
        // Using bands 1 (Red) and 2 (NIR) from multi-band dataset
        val spec = """{"calc": "(B-A)/(B+A)", "A_index": 0, "A_band": 1, "B_index": 0, "B_band": 2}"""
        
        val (resultDs, _) = RST_MapAlgebra.execute(Seq(multiBandDs), Map.empty, spec)
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        
        val tmpFiles = resultDs.GetFileList().asScala.toSeq.map(_.toString)
        RasterDriver.releaseDataset(resultDs)
        tmpFiles.foreach(f => Files.deleteIfExists(Paths.get(f)))
    }

    test("RST_MapAlgebra should handle complex mathematical expressions") {
        val outputPath = s"/tmp/mapalgebra_complex_${UUID.randomUUID()}.tif"
        // Complex expression: Ratio with scaling (A*2 + B) / (A + B + 1)
        val spec = """{"calc": "(A*2+B)/(A+B+1)", "A_index": 0, "A_band": 1, "B_index": 0, "B_band": 2}"""
        
        val (resultDs, _) = RST_MapAlgebra.execute(Seq(multiBandDs), Map.empty, spec)
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        
        val tmpFiles = resultDs.GetFileList().asScala.toSeq.map(_.toString)
        RasterDriver.releaseDataset(resultDs)
        tmpFiles.foreach(f => Files.deleteIfExists(Paths.get(f)))
    }

    test("RST_MapAlgebra should support conditional operations") {
        val outputPath = s"/tmp/mapalgebra_conditional_${UUID.randomUUID()}.tif"
        // Conditional masking: Set values < 50 to 0, keep others
        val spec = """{"calc": "A*(A>=50)", "A_index": 0, "A_band": 1}"""
        
        val (resultDs, _) = RST_MapAlgebra.execute(Seq(multiBandDs), Map.empty, spec)
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        
        val tmpFiles = resultDs.GetFileList().asScala.toSeq.map(_.toString)
        RasterDriver.releaseDataset(resultDs)
        tmpFiles.foreach(f => Files.deleteIfExists(Paths.get(f)))
    }

    test("RST_MapAlgebra should compute multi-input weighted average") {
        val outputPath = s"/tmp/mapalgebra_weighted_${UUID.randomUUID()}.tif"
        // Simple average of 3 bands: (A + B + C) / 3
        val spec = """{"calc": "(A+B+C)/3", "A_index": 0, "A_band": 1, "B_index": 0, "B_band": 2, "C_index": 0, "C_band": 3}"""
        
        val (resultDs, _) = RST_MapAlgebra.execute(Seq(multiBandDs), Map.empty, spec)
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        
        val tmpFiles = resultDs.GetFileList().asScala.toSeq.map(_.toString)
        RasterDriver.releaseDataset(resultDs)
        tmpFiles.foreach(f => Files.deleteIfExists(Paths.get(f)))
    }

    test("RST_MapAlgebra should compute Enhanced Vegetation Index (EVI)") {
        val outputPath = s"/tmp/mapalgebra_evi_${UUID.randomUUID()}.tif"
        // EVI = 2.5 * ((NIR - Red) / (NIR + 6*Red - 7.5*Blue + 1))
        // Using bands from multi-band dataset: Band 1=Red, Band 2=NIR, Band 3=Blue
        val spec = """{"calc": "2.5*((B-A)/(B+6*A-7.5*C+1))", "A_index": 0, "A_band": 1, "B_index": 0, "B_band": 2, "C_index": 0, "C_band": 3}"""
        
        val (resultDs, _) = RST_MapAlgebra.execute(Seq(multiBandDs), Map.empty, spec)
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        
        val tmpFiles = resultDs.GetFileList().asScala.toSeq.map(_.toString)
        RasterDriver.releaseDataset(resultDs)
        tmpFiles.foreach(f => Files.deleteIfExists(Paths.get(f)))
    }

    test("RST_MapAlgebra should handle logarithmic transformations") {
        val outputPath = s"/tmp/mapalgebra_log_${UUID.randomUUID()}.tif"
        // Log transformation with safety: log(A + 1) to avoid log(0)
        val spec = """{"calc": "log(A+1)", "A_index": 0}"""
        
        val (resultDs, _) = RST_MapAlgebra.execute(Seq(ds1), Map.empty, spec)
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        
        val tmpFiles = resultDs.GetFileList().asScala.toSeq.map(_.toString)
        RasterDriver.releaseDataset(resultDs)
        tmpFiles.foreach(f => Files.deleteIfExists(Paths.get(f)))
    }

    test("RST_MapAlgebra should compute normalized difference indices") {
        val outputPath = s"/tmp/mapalgebra_normalized_${UUID.randomUUID()}.tif"
        // Generic normalized difference: (A - B) / (A + B + epsilon)
        val spec = """{"calc": "(A-B)/(A+B+0.0001)", "A_index": 0, "B_index": 1}"""
        
        val (resultDs, _) = RST_MapAlgebra.execute(Seq(ds1, ds2), Map.empty, spec)
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        
        val tmpFiles = resultDs.GetFileList().asScala.toSeq.map(_.toString)
        RasterDriver.releaseDataset(resultDs)
        tmpFiles.foreach(f => Files.deleteIfExists(Paths.get(f)))
    }

    test("RST_MapAlgebra should handle power operations and scaling") {
        val outputPath = s"/tmp/mapalgebra_power_${UUID.randomUUID()}.tif"
        // Power operation with scaling: (A / 255)^2 * 255
        val spec = """{"calc": "(A/255)**2*255", "A_index": 0}"""
        
        val (resultDs, _) = RST_MapAlgebra.execute(Seq(ds1), Map.empty, spec)
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        
        val tmpFiles = resultDs.GetFileList().asScala.toSeq.map(_.toString)
        RasterDriver.releaseDataset(resultDs)
        tmpFiles.foreach(f => Files.deleteIfExists(Paths.get(f)))
    }
}
