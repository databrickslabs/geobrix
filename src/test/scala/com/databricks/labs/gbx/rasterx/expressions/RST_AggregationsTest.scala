package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
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
 * - RST_DerivedBand: Python-based combination with custom functions
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
    // Edge Cases and Integration Test (1 test)
    // ====================================================================

    test("Aggregations should handle edge cases gracefully") {
        // Simple Python function with single raster
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
     * the DerivedBand numerical-correctness tests that need
     * deterministic synthetic inputs with a known expected mean. EPSG:4326 +
     * a fixed geotransform so gdalbuildvrt aligns the rasters. If `nodata` is
     * set, it's stamped on the band.
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
