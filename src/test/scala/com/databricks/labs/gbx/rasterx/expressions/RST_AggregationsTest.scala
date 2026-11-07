package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.{BandAccessors, CombineAVG}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/**
 * Comprehensive tests for raster aggregation operations.
 * 
 * These tests validate:
 * - CombineAVG: Pixel-wise averaging of multiple rasters (the engine of gbx_rst_combineavg_agg)
 * - RST_DerivedBand: Python-based combination with custom functions
 * - RST_Merge: Spatial merging of rasters
 * - Edge cases: single raster, empty collections, NoData handling
 * - Performance: handling multiple large rasters
 * - Correctness: verifying output dimensions, metadata preservation
 * 
 * Test Data:
 * - MODIS single-band TIF files (B01, B02, B03)
 * - Consistent spatial extent and resolution for all inputs
 */
class RST_AggregationsTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds1: Dataset = _
    var ds2: Dataset = _
    var ds3: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        
        // Ensure NodeFilePathUtil root directory exists for tests
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
        
        // Load test datasets
        val tif1Path = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").getPath
        val tif2Path = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF").getPath
        val tif3Path = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B03.TIF").getPath
        
        ds1 = gdal.Open(tif1Path)
        ds2 = gdal.Open(tif2Path)
        ds3 = gdal.Open(tif3Path)
        
        require(ds1 != null, s"Failed to open $tif1Path")
        require(ds2 != null, s"Failed to open $tif2Path")
        require(ds3 != null, s"Failed to open $tif3Path")
    }

    override def afterAll(): Unit = {
        if (ds1 != null) ds1.delete()
        if (ds2 != null) ds2.delete()
        if (ds3 != null) ds3.delete()
    }

    // ====================================================================
    // CombineAVG Tests (3 tests)
    // ====================================================================

    /** The cell id is the tiles' shared id, or -1 when they differ; the pixels come from CombineAVG. */
    private def combineAvg(tiles: Seq[(Long, Dataset, Map[String, String])]): (Long, Dataset, Map[String, String]) = {
        val cellID = if (tiles.map(_._1).distinct.size == 1) tiles.head._1 else -1L
        val (combined, mtd) = CombineAVG.compute(tiles.map(_._2).toArray, tiles.head._3)
        (cellID, combined, mtd)
    }

    test("CombineAvg should average two identical rasters") {
        // Averaging two identical rasters should produce the same result
        val (idx, resultDs, metadata) = combineAvg(Seq((1L, ds1, Map.empty), (1L, ds1, Map.empty)))
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        resultDs.GetRasterXSize shouldBe ds1.GetRasterXSize
        resultDs.GetRasterYSize shouldBe ds1.GetRasterYSize
        
        // Verify spatial reference is preserved
        resultDs.GetProjection() should not be empty
        
        RasterDriver.releaseDataset(resultDs)
    }

    test("CombineAvg should average multiple different rasters") {
        // Average 3 different bands - result should have valid pixel values
        val (idx, resultDs, metadata) = combineAvg(Seq((1L, ds1, Map.empty), (1L, ds2, Map.empty), (1L, ds3, Map.empty)))
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        resultDs.GetRasterXSize shouldBe ds1.GetRasterXSize
        resultDs.GetRasterYSize shouldBe ds1.GetRasterYSize
        
        // Verify geotransform is preserved
        val gt = Array.ofDim[Double](6)
        resultDs.GetGeoTransform(gt)
        gt(1) should not be 0.0 // pixel width
        gt(5) should not be 0.0 // pixel height
        
        RasterDriver.releaseDataset(resultDs)
    }

    test("CombineAvg actually averages pixel values (regression: pixel function must fire)") {
        // Synthetic control with known answer. Two constant Byte rasters
        // (50 and 100); the mean must be 75 everywhere. Before the
        // PixelCombineRasters ordering fix, gdal.Open parsed the VRT XML
        // BEFORE addPixelFunction wrote PixelFunctionLanguage into the file,
        // so the in-memory Dataset never saw the Python function and
        // gdal.Translate fell back to a default multi-source mosaic
        // (last-source-wins per pixel). On co-extensive inputs the output
        // then equaled the last input — 50 or 100, never 75 — silently and
        // without any error. This test would fail (output 100 instead of
        // 75); after the fix it passes.
        val tmpDir = Files.createTempDirectory("gbx_combineavg_").toFile
        val const50  = makeConstantByteRaster(s"${tmpDir.getAbsolutePath}/const_50.tif",  50)
        val const100 = makeConstantByteRaster(s"${tmpDir.getAbsolutePath}/const_100.tif", 100)
        try {
            val (_, resultDs, _) = combineAvg(
                Seq((1L, const50, Map.empty), (1L, const100, Map.empty))
            )
            try {
                val w = resultDs.GetRasterXSize
                val h = resultDs.GetRasterYSize
                val band = resultDs.GetRasterBand(1)
                val buf = Array.ofDim[Double](w * h)
                band.ReadRaster(0, 0, w, h, gdalconstConstants.GDT_Float64, buf)
                buf.min shouldBe 75.0 +- 0.5
                buf.max shouldBe 75.0 +- 0.5
                (buf.sum / buf.length) shouldBe 75.0 +- 0.5
            } finally RasterDriver.releaseDataset(resultDs)
        } finally {
            RasterDriver.releaseDataset(const50)
            RasterDriver.releaseDataset(const100)
            tmpDir.listFiles().foreach(_.delete())
            tmpDir.delete()
        }
    }

    test("CombineAvg excludes declared NoData from both sum and divisor") {
        // Two 4x4 Byte rasters, NoData=255 declared on input A:
        //   A: half of cells are 100, half are 255 (NoData)
        //   B: all cells are 50
        // Expected output per cell:
        //   - A=100 cells: mean(100, 50) = 75
        //   - A=255 cells: A excluded, mean = 50
        // Before fix #2 the pyfunc summed 255 in (sum=305, div=2) → 152.
        val tmpDir = Files.createTempDirectory("gbx_combineavg_nodata_").toFile
        val w = 4; val h = 4
        val aVals = Array[Byte](
            100, 100, 100, 100,
            100, 100, 100, 100,
            -1, -1, -1, -1,   // 0xFF = 255
            -1, -1, -1, -1
        )
        val bVals = Array.fill[Byte](w * h)(50)
        val a = makeByteRaster(s"${tmpDir.getAbsolutePath}/a.tif", aVals, w, h, nodata = Some(255))
        val b = makeByteRaster(s"${tmpDir.getAbsolutePath}/b.tif", bVals, w, h, nodata = None)
        try {
            val (_, resultDs, _) = combineAvg(
                Seq((1L, a, Map.empty), (1L, b, Map.empty))
            )
            try {
                val band = resultDs.GetRasterBand(1)
                val buf = Array.ofDim[Double](w * h)
                band.ReadRaster(0, 0, w, h, gdalconstConstants.GDT_Float64, buf)
                // Top half: both inputs valid → 75
                buf.slice(0, 8).forall(v => math.abs(v - 75.0) <= 0.5) shouldBe true
                // Bottom half: A is NoData, only B contributes → 50
                buf.slice(8, 16).forall(v => math.abs(v - 50.0) <= 0.5) shouldBe true
                // Output NoData should be stamped (sourced from input A's 255).
                val outNd = BandAccessors.getNoDataValue(band)
                outNd shouldBe 255.0 +- 0.5
            } finally RasterDriver.releaseDataset(resultDs)
        } finally {
            RasterDriver.releaseDataset(a)
            RasterDriver.releaseDataset(b)
            tmpDir.listFiles().foreach(_.delete())
            tmpDir.delete()
        }
    }

    test("CombineAvg counts valid 0 cells in the divisor") {
        // Two 4x4 Byte rasters, NO NoData declared on either:
        //   A: all cells = 0  (a valid measurement of zero, not NoData)
        //   B: all cells = 100
        // Expected output: 50 everywhere — both contribute to divisor.
        // Before fix #2 the pyfunc divisor was `np.sum(stacked > 0, axis=0)`
        // which counted only B → output 100 everywhere.
        val tmpDir = Files.createTempDirectory("gbx_combineavg_zero_").toFile
        val w = 4; val h = 4
        val a = makeByteRaster(s"${tmpDir.getAbsolutePath}/a.tif", Array.fill[Byte](w * h)(0),   w, h, nodata = None)
        val b = makeByteRaster(s"${tmpDir.getAbsolutePath}/b.tif", Array.fill[Byte](w * h)(100), w, h, nodata = None)
        try {
            val (_, resultDs, _) = combineAvg(
                Seq((1L, a, Map.empty), (1L, b, Map.empty))
            )
            try {
                val w2 = resultDs.GetRasterXSize
                val h2 = resultDs.GetRasterYSize
                val buf = Array.ofDim[Double](w2 * h2)
                resultDs.GetRasterBand(1).ReadRaster(0, 0, w2, h2, gdalconstConstants.GDT_Float64, buf)
                buf.min shouldBe 50.0 +- 0.5
                buf.max shouldBe 50.0 +- 0.5
            } finally RasterDriver.releaseDataset(resultDs)
        } finally {
            RasterDriver.releaseDataset(a)
            RasterDriver.releaseDataset(b)
            tmpDir.listFiles().foreach(_.delete())
            tmpDir.delete()
        }
    }

    test("CombineAvg rounds (not truncates) when casting to integer output") {
        // mean(50, 99) = 74.5; with truncation that becomes 74, with rounding 74
        // (banker's rounding → nearest even). Pick mean(51, 100) = 75.5 → 76.
        // Before fix #2: `np.divide(..., casting='unsafe')` truncated → 75.
        val tmpDir = Files.createTempDirectory("gbx_combineavg_round_").toFile
        val w = 4; val h = 4
        val a = makeByteRaster(s"${tmpDir.getAbsolutePath}/a.tif", Array.fill[Byte](w * h)(51),  w, h, nodata = None)
        val b = makeByteRaster(s"${tmpDir.getAbsolutePath}/b.tif", Array.fill[Byte](w * h)(100), w, h, nodata = None)
        try {
            val (_, resultDs, _) = combineAvg(
                Seq((1L, a, Map.empty), (1L, b, Map.empty))
            )
            try {
                val w2 = resultDs.GetRasterXSize
                val h2 = resultDs.GetRasterYSize
                val buf = Array.ofDim[Double](w2 * h2)
                resultDs.GetRasterBand(1).ReadRaster(0, 0, w2, h2, gdalconstConstants.GDT_Float64, buf)
                buf.min shouldBe 76.0 +- 0.5
                buf.max shouldBe 76.0 +- 0.5
            } finally RasterDriver.releaseDataset(resultDs)
        } finally {
            RasterDriver.releaseDataset(a)
            RasterDriver.releaseDataset(b)
            tmpDir.listFiles().foreach(_.delete())
            tmpDir.delete()
        }
    }

    test("CombineAvg should generate operation metadata") {
        val inputMetadata = Map(
          "TEST_KEY" -> "TEST_VALUE",
          "PROCESSING" -> "AVERAGE"
        )
        
        val (idx, resultDs, metadata) = combineAvg(Seq((1L, ds1, inputMetadata), (1L, ds2, inputMetadata)))
        
        resultDs should not be null
        // Verify operation generates internal metadata
        metadata should contain key "path"
        metadata should contain key "driver"
        metadata should contain key "format"
        
        RasterDriver.releaseDataset(resultDs)
    }

    // ====================================================================
    // RST_DerivedBand Tests (3 tests)
    // ====================================================================

    test("DerivedBand actually transforms pixel values (regression: pixel function must fire)") {
        // Synthetic doubling — confirms the user-supplied pyfunc fires through
        // the same PixelCombineRasters path as CombineAvg. Before the fix #1
        // ordering bug was repaired, the pyfunc never executed and gdal.Translate
        // returned a multi-source mosaic of the inputs, so a single-input
        // doubling pyfunc would have silently returned the input unchanged.
        val tmpDir = Files.createTempDirectory("gbx_derivedband_double_").toFile
        val input = makeConstantByteRaster(s"${tmpDir.getAbsolutePath}/in.tif", 50)
        val pyfunc = """
                       |import numpy as np
                       |def doubleit(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
                       |    out_ar[:] = np.asarray(in_ar[0], dtype=np.float64) * 2
                       |""".stripMargin
        try {
            val (resultDs, _) = RST_DerivedBand.execute(Seq(input), Map.empty, pyfunc, "doubleit")
            try {
                val w = resultDs.GetRasterXSize
                val h = resultDs.GetRasterYSize
                val buf = Array.ofDim[Double](w * h)
                resultDs.GetRasterBand(1).ReadRaster(0, 0, w, h, gdalconstConstants.GDT_Float64, buf)
                buf.min shouldBe 100.0 +- 0.5
                buf.max shouldBe 100.0 +- 0.5
            } finally RasterDriver.releaseDataset(resultDs)
        } finally {
            RasterDriver.releaseDataset(input)
            tmpDir.listFiles().foreach(_.delete())
            tmpDir.delete()
        }
    }

    test("DerivedBand averages multi-input pixel values") {
        // Three co-extensive inputs 50 / 100 / 150 with a numpy-mean pyfunc;
        // expected output 100 everywhere. Equivalent to CombineAvg on the
        // simple case but exercising the user-pyfunc code path.
        val tmpDir = Files.createTempDirectory("gbx_derivedband_avg_").toFile
        val a = makeConstantByteRaster(s"${tmpDir.getAbsolutePath}/a.tif",  50)
        val b = makeConstantByteRaster(s"${tmpDir.getAbsolutePath}/b.tif", 100)
        val c = makeConstantByteRaster(s"${tmpDir.getAbsolutePath}/c.tif", 150)
        val pyfunc = """
                       |import numpy as np
                       |def meanof(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
                       |    out_ar[:] = np.mean(np.asarray(in_ar, dtype=np.float64), axis=0)
                       |""".stripMargin
        try {
            val (resultDs, _) = RST_DerivedBand.execute(Seq(a, b, c), Map.empty, pyfunc, "meanof")
            try {
                val w = resultDs.GetRasterXSize
                val h = resultDs.GetRasterYSize
                val buf = Array.ofDim[Double](w * h)
                resultDs.GetRasterBand(1).ReadRaster(0, 0, w, h, gdalconstConstants.GDT_Float64, buf)
                buf.min shouldBe 100.0 +- 0.5
                buf.max shouldBe 100.0 +- 0.5
            } finally RasterDriver.releaseDataset(resultDs)
        } finally {
            RasterDriver.releaseDataset(a)
            RasterDriver.releaseDataset(b)
            RasterDriver.releaseDataset(c)
            tmpDir.listFiles().foreach(_.delete())
            tmpDir.delete()
        }
    }

    test("DerivedBand should apply simple Python averaging function") {
        val pyfunc = """
import numpy as np
def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    # Simple average across all input rasters
    stacked = np.array(in_ar)
    out_ar[:] = np.mean(stacked, axis=0)
"""
        
        val (resultDs, metadata) = RST_DerivedBand.execute(Seq(ds1, ds2, ds3), Map.empty, pyfunc, "average")
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        resultDs.GetRasterXSize shouldBe ds1.GetRasterXSize
        resultDs.GetRasterYSize shouldBe ds1.GetRasterYSize
        
        RasterDriver.releaseDataset(resultDs)
    }

    test("DerivedBand should apply maximum value selection") {
        val pyfunc = """
import numpy as np
def select_max(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    # Select maximum value across all input rasters
    stacked = np.array(in_ar)
    out_ar[:] = np.max(stacked, axis=0)
"""
        
        val (resultDs, metadata) = RST_DerivedBand.execute(Seq(ds1, ds2), Map.empty, pyfunc, "select_max")
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        
        RasterDriver.releaseDataset(resultDs)
    }

    test("DerivedBand should apply weighted combination") {
        val pyfunc = """
import numpy as np
def weighted_avg(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    # Weighted average: first raster gets 50%, others split remaining 50%
    stacked = np.array(in_ar)
    n = len(in_ar)
    weights = [0.5] + [0.5/(n-1)] * (n-1)
    out_ar[:] = np.average(stacked, axis=0, weights=weights)
"""
        
        val (resultDs, metadata) = RST_DerivedBand.execute(Seq(ds1, ds2, ds3), Map.empty, pyfunc, "weighted_avg")
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        
        RasterDriver.releaseDataset(resultDs)
    }

    // ====================================================================
    // RST_Merge Tests (3 tests)
    // ====================================================================

    test("Merge should combine two overlapping rasters") {
        // Merging identical rasters should result in same or larger dimensions
        val (resultDs, metadata) = RST_Merge.execute(Array(ds1, ds1), Map.empty)
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        resultDs.GetRasterXSize should be >= ds1.GetRasterXSize
        resultDs.GetRasterYSize should be >= ds1.GetRasterYSize
        
        // Verify projection is preserved
        val projection = resultDs.GetProjection()
        projection should not be empty
        
        RasterDriver.releaseDataset(resultDs)
    }

    test("Merge should handle multiple rasters with same extent") {
        // When rasters have same extent, merge should preserve or expand dimensions
        val (resultDs, metadata) = RST_Merge.execute(Array(ds1, ds2, ds3), Map.empty)
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        
        // For identical extents, dimensions should be at least as large
        resultDs.GetRasterXSize should be >= ds1.GetRasterXSize
        resultDs.GetRasterYSize should be >= ds1.GetRasterYSize
        
        // Verify geotransform is valid
        val gtResult = Array.ofDim[Double](6)
        resultDs.GetGeoTransform(gtResult)
        
        gtResult(1) should not be 0.0 // pixel width
        gtResult(5) should not be 0.0 // pixel height
        
        RasterDriver.releaseDataset(resultDs)
    }

    test("Merge should generate operation metadata") {
        val inputMetadata = Map(
          "SOURCE" -> "MODIS",
          "PRODUCT" -> "MCD43A4"
        )
        
        val (resultDs, metadata) = RST_Merge.execute(Array(ds1, ds2), inputMetadata)
        
        resultDs should not be null
        // Verify operation generates internal metadata
        metadata should contain key "path"
        metadata should contain key "driver"
        metadata should contain key "all_parents" // merge tracks parent files
        
        RasterDriver.releaseDataset(resultDs)
    }

    // ====================================================================
    // Edge Cases and Integration Test (1 test)
    // ====================================================================

    test("Aggregations should handle edge cases gracefully") {
        // Test 1: Single raster in CombineAvg (should return same raster)
        val (singleIdx, singleDs, _) = combineAvg(Seq((1L, ds1, Map.empty)))
        singleDs should not be null
        singleDs.GetRasterXSize shouldBe ds1.GetRasterXSize
        RasterDriver.releaseDataset(singleDs)
        
        // Test 2: Merge single raster (should return same raster)
        val (mergedSingle, _) = RST_Merge.execute(Array(ds1), Map.empty)
        mergedSingle should not be null
        mergedSingle.GetRasterXSize should be >= ds1.GetRasterXSize
        RasterDriver.releaseDataset(mergedSingle)
        
        // Test 3: Simple Python function with single raster
        val simplePyfunc = """
import numpy as np
def identity(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    out_ar[:] = in_ar[0]
"""
        val (derivedDs, _) = RST_DerivedBand.execute(Seq(ds1), Map.empty, simplePyfunc, "identity")
        derivedDs should not be null
        RasterDriver.releaseDataset(derivedDs)
    }

    private def makeConstantByteRaster(path: String, value: Int, w: Int = 32, h: Int = 32): Dataset =
        makeByteRaster(path, Array.fill[Byte](w * h)(value.toByte), w, h, nodata = None)

    /**
     * Create a small GTiff backed by `values` (length w*h, row-major). Used by
     * the CombineAvg / DerivedBand numerical-correctness tests that need
     * deterministic synthetic inputs with a known expected mean. EPSG:4326 +
     * a fixed geotransform so gdalbuildvrt aligns the rasters. If `nodata` is
     * set, it's stamped on the band so the CombineAVG pyfunc can mask those
     * cells out of both sum and divisor.
     *
     * Note: closes the writer Dataset and re-opens it for reading. Without
     * this, downstream gdalbuildvrt sometimes sees an unflushed file and
     * pulls in zeros for the pixel values. Also uses the byte[] overload of
     * WriteRaster — the int[]/GDT_Byte combo silently writes nothing.
     */
    private def makeByteRaster(path: String, values: Array[Byte], w: Int, h: Int, nodata: Option[Int]): Dataset = {
        require(values.length == w * h, s"expected ${w * h} values, got ${values.length}")
        val drv = gdal.GetDriverByName("GTiff")
        val writer = drv.Create(path, w, h, 1, gdalconstConstants.GDT_Byte, Array[String]("COMPRESS=DEFLATE"))
        writer.SetGeoTransform(Array[Double](149.0, 0.01, 0.0, -35.0, 0.0, -0.01))
        val sr = new SpatialReference()
        sr.ImportFromEPSG(4326)
        writer.SetProjection(sr.ExportToWkt())
        val band = writer.GetRasterBand(1)
        nodata.foreach(nd => band.SetNoDataValue(nd.toDouble))
        band.WriteRaster(0, 0, w, h, values)
        band.FlushCache()
        writer.FlushCache()
        writer.delete()
        gdal.Open(path)
    }
}
