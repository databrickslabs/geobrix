package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files
import java.util.UUID

/**
  * Direct-execute unit tests for [[RST_BinPoints]].
  *
  * All tests call `RST_BinPoints.execute(...)` directly with `Array[Double]` inputs —
  * no Spark session or JAR required. GDAL is initialised once in [[beforeAll]].
  *
  * Row layout for the shared 2×2 fixture
  * ----------------------------------------
  * Grid: [0,2]×[0,2], 2 pixels wide × 2 pixels tall, EPSG:4326.
  * Raster convention: row 0 = top (max y).
  * - (0.5, 1.5): row = floor((2-1.5)/2*2) = 0, col = floor((0.5-0)/2*2) = 0 → top-left [0]
  * - (1.5, 0.5): row = floor((2-0.5)/2*2) = 1, col = floor((1.5-0)/2*2) = 1 → bottom-right [3]
  * row-major index = row*w + col.
  */
class RST_BinPointsTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    /**
      * Read all Float32 band-1 values (row-major) from GTiff bytes, plus dtype and NoData.
      * Opens via /vsimem/ and cleans up on exit.
      */
    private def readBand(bytes: Array[Byte]): (Array[Float], Int, Double) = {
        val path = s"/vsimem/tbp_${UUID.randomUUID().toString.replace("-", "")}.tif"
        gdal.FileFromMemBuffer(path, bytes)
        val ds = gdal.Open(path)
        try {
            val w    = ds.GetRasterXSize
            val h    = ds.GetRasterYSize
            val buf  = new Array[Float](w * h)
            val band = ds.GetRasterBand(1)
            band.ReadRaster(0, 0, w, h, buf)
            val dtype = band.getDataType
            val ndArr = new Array[java.lang.Double](1)
            band.GetNoDataValue(ndArr)
            val nodata = if (ndArr(0) == null) Double.NaN else ndArr(0).doubleValue
            (buf, dtype, nodata)
        } finally {
            ds.delete()
            gdal.Unlink(path)
        }
    }

    // --- shared 2×2 fixture ---
    private val x2   = Array(0.5, 0.5, 1.5)
    private val y2   = Array(1.5, 1.5, 0.5)
    private val z2   = Array(10.0, 25.0, 7.0)
    private val xmin = 0.0; private val ymin = 0.0
    private val xmax = 2.0; private val ymax = 2.0
    private val w    = 2;   private val h    = 2
    private val epsg = 4326

    // --- max (default) ---

    test("RST_BinPoints: max — two-point cell → max(10,25)=25; single-point cell → 7; empties → -9999") {
        val bytes = RST_BinPoints.execute(x2, y2, z2, xmin, ymin, xmax, ymax, w, h, epsg, "max")
        bytes should not be null
        val (band, _, _) = readBand(bytes)
        band(0) shouldBe 25.0f   // top-left:     max(10,25)
        band(3) shouldBe 7.0f    // bottom-right: 7
        band(1) shouldBe -9999.0f // top-right:   empty
        band(2) shouldBe -9999.0f // bottom-left: empty
    }

    // --- min ---

    test("RST_BinPoints: min — two-point cell → min(10,25)=10") {
        val bytes = RST_BinPoints.execute(x2, y2, z2, xmin, ymin, xmax, ymax, w, h, epsg, "min")
        val (band, _, _) = readBand(bytes)
        band(0) shouldBe 10.0f
        band(3) shouldBe 7.0f
        band(1) shouldBe -9999.0f
        band(2) shouldBe -9999.0f
    }

    // --- mean ---

    test("RST_BinPoints: mean — two-point cell → mean(10,25)=17.5") {
        val bytes = RST_BinPoints.execute(x2, y2, z2, xmin, ymin, xmax, ymax, w, h, epsg, "mean")
        val (band, _, _) = readBand(bytes)
        band(0) shouldBe 17.5f
        band(3) shouldBe 7.0f
    }

    // --- count ---

    test("RST_BinPoints: count — two-point cell → 2.0; single-point cell → 1.0; empty → -9999") {
        val bytes = RST_BinPoints.execute(x2, y2, z2, xmin, ymin, xmax, ymax, w, h, epsg, "count")
        val (band, _, _) = readBand(bytes)
        band(0) shouldBe 2.0f
        band(3) shouldBe 1.0f
        band(1) shouldBe -9999.0f
        band(2) shouldBe -9999.0f
    }

    // --- median ---

    test("RST_BinPoints: median — 3 points (z=1,2,3) in a 1×1 cell → numpy median = 2.0") {
        // numpy.percentile([1,2,3], 50): idx = 0.5*(3-1) = 1.0 → sorted[1] = 2.0
        val bytes = RST_BinPoints.execute(
            Array(0.5, 0.5, 0.5), Array(0.5, 0.5, 0.5), Array(1.0, 2.0, 3.0),
            0.0, 0.0, 1.0, 1.0, 1, 1, epsg, "median"
        )
        val (band, _, _) = readBand(bytes)
        band(0) shouldBe (2.0f +- 0.001f)
    }

    // --- percentile:90 ---

    test("RST_BinPoints: percentile:90 — 3 points (z=1,2,3) → numpy linear-interp = 2.8") {
        // numpy.percentile([1,2,3], 90): idx = 0.9*(3-1) = 1.8 → 2 + 0.8*(3-2) = 2.8
        val bytes = RST_BinPoints.execute(
            Array(0.5, 0.5, 0.5), Array(0.5, 0.5, 0.5), Array(1.0, 2.0, 3.0),
            0.0, 0.0, 1.0, 1.0, 1, 1, epsg, "percentile:90"
        )
        val (band, _, _) = readBand(bytes)
        band(0) shouldBe (2.8f +- 0.001f)
    }

    // --- half-open drop: point at exactly x=xmax is excluded ---

    test("RST_BinPoints: half-open drop — point at x=xmax (col==w) is excluded; in-bounds point remains") {
        // Point (1.0, 0.5, 99.0): col = floor((1.0-0)/1*1) = 1 >= w=1 → EXCLUDED
        // Point (0.5, 0.5, 42.0): col = floor(0.5) = 0 < 1 → INCLUDED
        // If both were included, max = 99; since boundary is dropped, cell = 42.
        val bytes = RST_BinPoints.execute(
            Array(1.0, 0.5), Array(0.5, 0.5), Array(99.0, 42.0),
            0.0, 0.0, 1.0, 1.0, 1, 1, epsg, "max"
        )
        val (band, _, _) = readBand(bytes)
        band(0) shouldBe 42.0f  // boundary point excluded; only in-bounds contributes
    }

    // --- out-of-bounds drop ---

    test("RST_BinPoints: out-of-bounds drop — point at x<xmin excluded; in-bounds point unaffected") {
        // Point (-0.5, 0.5, 99.0): col = floor((-0.5-0)/1*1) = -1 < 0 → EXCLUDED
        // Point (0.5,  0.5, 42.0): col = 0 → INCLUDED
        val bytes = RST_BinPoints.execute(
            Array(-0.5, 0.5), Array(0.5, 0.5), Array(99.0, 42.0),
            0.0, 0.0, 1.0, 1.0, 1, 1, epsg, "max"
        )
        val (band, _, _) = readBand(bytes)
        band(0) shouldBe 42.0f
    }

    // --- output dtype and NoData ---

    test("RST_BinPoints: output band is Float32 with NoData=-9999") {
        val bytes = RST_BinPoints.execute(x2, y2, z2, xmin, ymin, xmax, ymax, w, h, epsg, "max")
        val (_, dtype, nodata) = readBand(bytes)
        dtype  shouldBe gdalconstConstants.GDT_Float32
        nodata shouldBe -9999.0
    }

    // --- empty input ---

    test("RST_BinPoints: empty x array → execute returns null") {
        val result = RST_BinPoints.execute(
            Array.empty[Double], Array.empty[Double], Array.empty[Double],
            xmin, ymin, xmax, ymax, w, h, epsg, "max"
        )
        result shouldBe null
    }

}
