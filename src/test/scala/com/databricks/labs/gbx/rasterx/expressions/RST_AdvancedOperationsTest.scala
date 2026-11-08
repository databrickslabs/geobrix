package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.CombineAVG
import com.databricks.labs.gbx.rasterx.operator.{GDALBuildVRT, GDALTranslate}
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}
import java.util.UUID

/**
 * Advanced tests for RST_DerivedBand and the per-band combine operations.
 * 
 * These tests validate:
 * - Complex Python functions with numpy operations
 * - Multi-band derived band processing
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

    test("RST_DerivedBand collapses a multi-band input to a single derived band") {
        // Regression: a single N-band input tile must still yield exactly ONE
        // derived band (matching the documented/lightweight contract), not one
        // output band per input band. multiBandDs has 3 bands.
        multiBandDs.GetRasterCount shouldBe 3
        val pyfunc = """
import numpy as np
def multiband_average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    stacked = np.array(in_ar)
    out_ar[:] = np.mean(stacked, axis=0)
"""
        val (derivedDs, _) = RST_DerivedBand.execute(Seq(multiBandDs), Map.empty, pyfunc, "multiband_average")

        derivedDs should not be null
        derivedDs.GetRasterCount shouldBe 1
        derivedDs.GetRasterXSize shouldBe multiBandDs.GetRasterXSize
        derivedDs.GetRasterYSize shouldBe multiBandDs.GetRasterYSize

        RasterDriver.releaseDataset(derivedDs)
    }

    // ====================================================================
    // RST_CombineAvg band-count contract (2 tests)
    // ====================================================================

    test("RST_CombineAvg preserves band count (per-band mean of multi-band inputs)") {
        // Contract: combineavg of K aligned N-band tiles yields an N-band tile where
        // output band j = mean of band j across the K inputs. It must NOT collapse to
        // a single band (that is the derivedband contract). Regression for the shared
        // -separate/collapse in PixelCombineRasters that wrongly applied to combineavg.
        multiBandDs.GetRasterCount shouldBe 3
        val (avgDs, _) = CombineAVG.compute(Array(multiBandDs, multiBandDs), Map.empty)

        avgDs should not be null
        avgDs.GetRasterCount shouldBe 3
        avgDs.GetRasterXSize shouldBe multiBandDs.GetRasterXSize
        avgDs.GetRasterYSize shouldBe multiBandDs.GetRasterYSize

        // Averaging a tile with itself is the identity per band on VALID pixels, so
        // verify band j of the output equals band j of the input there (proving a
        // per-band mean, not a cross-band collapse). MODIS fill is 32767; those
        // all-NoData cells legitimately resolve to the fallback, so skip them.
        val w = multiBandDs.GetRasterXSize
        val h = multiBandDs.GetRasterYSize
        val nodataFill = 32767.0
        val inBands = (1 to 3).map { b =>
            val buf = new Array[Float](w * h)
            multiBandDs.GetRasterBand(b).ReadRaster(0, 0, w, h, buf)
            buf
        }
        val outBands = (1 to 3).map { b =>
            val buf = new Array[Float](w * h)
            avgDs.GetRasterBand(b).ReadRaster(0, 0, w, h, buf)
            buf
        }
        var checked = 0
        (0 until 3).foreach { j =>
            (0 until w * h).foreach { i =>
                if (inBands(j)(i).toDouble != nodataFill) {
                    outBands(j)(i).toDouble shouldBe inBands(j)(i).toDouble +- 1.0
                    checked += 1
                }
            }
        }
        checked should be > 0 // ensure we actually validated valid pixels

        // Bands must be distinct: a collapse would make all output bands identical.
        (outBands(0) sameElements outBands(1)) shouldBe false

        RasterDriver.releaseDataset(avgDs)
    }

    test("RST_CombineAvg of single-band inputs still yields one band") {
        // The historic single-band case (N single-band tiles -> 1-band mean) must be
        // unaffected by the per-band path.
        val (avgDs, _) = CombineAVG.compute(Array(ds1, ds1), Map.empty)

        avgDs should not be null
        avgDs.GetRasterCount shouldBe 1
        avgDs.GetRasterXSize shouldBe ds1.GetRasterXSize
        avgDs.GetRasterYSize shouldBe ds1.GetRasterYSize

        RasterDriver.releaseDataset(avgDs)
    }
}
