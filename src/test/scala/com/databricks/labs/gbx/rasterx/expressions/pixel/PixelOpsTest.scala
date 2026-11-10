package com.databricks.labs.gbx.rasterx.expressions.pixel

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Direct-execute tests for the pixel-ops + extraction expressions.
 *
 *  Each test runs `execute(...)` against a small synthetic MEM/GTiff raster —
 *  no Spark session bootstrap, ~1s per test. One happy-path test per function
 *  plus one shared "fail-loudly" assertion for invalid argument values.
 */
class PixelOpsTest extends AnyFunSuite with BeforeAndAfterAll {

    private var resultsBuf: List[Dataset] = List.empty

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    override def afterAll(): Unit = {
        resultsBuf.foreach { d => try d.delete() catch { case _: Throwable => () } }
    }

    private def track(t: (Dataset, Map[String, String])): (Dataset, Map[String, String]) = {
        resultsBuf = t._1 :: resultsBuf
        t
    }

    // ------------------------------------------------------------------
    // Synthetic raster helpers — UTM 32633, 1 m pixel, projected metric CRS.
    // ------------------------------------------------------------------

    /** Single-band Float32 raster of size width x height with `valueFn(col, row)` per pixel.
      *
      * Persists to a local path (not `/vsimem/`) so tests that shell out to a GDAL command can read the file.
      */
    private def buildRaster(
        width: Int, height: Int,
        valueFn: (Int, Int) => Float,
        nodata: Option[Double] = None
    ): Dataset = {
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        val driver = gdal.GetDriverByName("GTiff")
        val path = s"${NodeFilePathUtil.rootPath}/pixelops_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val ds = driver.Create(path, width, height, 1, gdalconstConstants.GDT_Float32)
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(32633)
        ds.SetProjection(sr.ExportToWkt())
        sr.delete()
        ds.SetGeoTransform(Array(500000.0, 1.0, 0.0, 5000000.0, 0.0, -1.0))
        val band = ds.GetRasterBand(1)
        nodata.foreach(nd => band.SetNoDataValue(nd))
        val buf = new Array[Float](width * height)
        var r = 0
        while (r < height) {
            var c = 0
            while (c < width) {
                buf(r * width + c) = valueFn(c, r)
                c += 1
            }
            r += 1
        }
        band.WriteRaster(0, 0, width, height, buf)
        band.FlushCache()
        ds.FlushCache()
        ds
    }

    /** 3-band Byte raster — each band's pixel value = bandIndex (1, 2, 3). */
    private def buildMultiBandRaster(width: Int, height: Int): Dataset = {
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        val driver = gdal.GetDriverByName("GTiff")
        val path = s"${NodeFilePathUtil.rootPath}/multiband_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val ds = driver.Create(path, width, height, 3, gdalconstConstants.GDT_Byte)
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(32633)
        ds.SetProjection(sr.ExportToWkt())
        sr.delete()
        ds.SetGeoTransform(Array(500000.0, 1.0, 0.0, 5000000.0, 0.0, -1.0))
        var b = 1
        while (b <= 3) {
            val band = ds.GetRasterBand(b)
            val buf = Array.fill[Byte](width * height)(b.toByte)
            band.WriteRaster(0, 0, width, height, buf)
            band.FlushCache()
            b += 1
        }
        ds.FlushCache()
        ds
    }

    private def pixel(ds: Dataset, col: Int, row: Int, band: Int = 1): Double = {
        val buf = new Array[Double](1)
        ds.GetRasterBand(band).ReadRaster(col, row, 1, 1, buf)
        buf(0)
    }

    // ------------------------------------------------------------------
    // Per-function happy-path tests.
    // ------------------------------------------------------------------

    test("RST_SetSrid stamps the requested EPSG code on the output without warping pixels") {
        import com.databricks.labs.gbx.rasterx.operations.SpatialRefOps
        val src = buildRaster(10, 10, (c, _) => c.toFloat) // CRS already 32633
        try {
            // Stamp 4326 (WGS84) — pixel data should NOT change, only the SR header.
            val (out, _) = track(RST_SetSrid.execute(src, Map.empty, 4326))
            out should not be null
            val outSR = out.GetSpatialRef
            outSR should not be null
            SpatialRefOps.getEPSGCode(outSR) shouldBe 4326
            // Pixel data preserved (still a west-to-east ramp).
            pixel(out, 0, 0) shouldBe 0.0 +- 1e-6
            pixel(out, 9, 0) shouldBe 9.0 +- 1e-6
        } finally {
            src.delete()
        }
    }

    test("RST_SetSrid stamps a non-EPSG ESRI code (54008) as ESRI, not a mislabel") {
        import com.databricks.labs.gbx.rasterx.operations.SpatialRefOps
        val src = buildRaster(4, 3, (c, _) => c.toFloat) // CRS already 32633
        try {
            // 54008 = ESRI World Sinusoidal (no EPSG code). ImportFromEPSG auto-recovers
            // it to ESRI:54008, so the stamped SR must carry the ESRI authority.
            val (out, _) = track(RST_SetSrid.execute(src, Map.empty, 54008))
            out should not be null
            SpatialRefOps.crsToCanonical(out.GetSpatialRef) shouldBe "ESRI:54008"
        } finally {
            src.delete()
        }
    }

    test("RST_SetSrid(0) clears the CRS (dumb-storage 'no CRS'), no exception") {
        val src = buildRaster(4, 3, (c, _) => c.toFloat) // CRS already 32633
        try {
            val (out, _) = track(RST_SetSrid.execute(src, Map.empty, 0))
            out should not be null
            // SR header cleared — no spatial reference on the output.
            out.GetSpatialRef shouldBe null
        } finally {
            src.delete()
        }
    }

    test("RST_SetSrid rejects a negative SRID (the one set-time bound)") {
        val src = buildRaster(4, 3, (c, _) => c.toFloat)
        try {
            an[IllegalArgumentException] should be thrownBy RST_SetSrid.execute(src, Map.empty, -1)
        } finally {
            src.delete()
        }
    }

    test("RST_SetSrid raises for a positive code in neither EPSG nor ESRI (apply moment)") {
        val src = buildRaster(4, 3, (c, _) => c.toFloat)
        try {
            an[IllegalArgumentException] should be thrownBy
                RST_SetSrid.execute(src, Map.empty, 99999999)
        } finally {
            src.delete()
        }
    }

    test("RST_Band extracts a specific band from a multi-band raster") {
        val src = buildMultiBandRaster(10, 10)
        try {
            // Band 2 has constant value 2 across every pixel.
            val (out, _) = track(RST_Band.execute(src, Map.empty, 2))
            out should not be null
            out.GetRasterCount shouldBe 1
            pixel(out, 5, 5) shouldBe 2.0 +- 1e-6
            pixel(out, 0, 0) shouldBe 2.0 +- 1e-6

            // Out-of-range band index should fail loudly.
            an[IllegalArgumentException] should be thrownBy {
                RST_Band.execute(src, Map.empty, 99)
            }
        } finally {
            src.delete()
        }
    }

}
