package com.databricks.labs.gbx.rasterx.expressions.dem

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Direct-execute tests for the terrain-analysis expression (TRI) and its helper.
 *
 *  We exercise each expression's pure compute path (`execute(...)`) on a small
 *  100x100 synthetic DEM. That avoids a full Spark session bootstrap and keeps
 *  each test under ~1s wall-clock.
 *
 *  The synthetic DEM is a linear west-to-east ramp from 0 to 100 m elevation
 *  (1 m per pixel), placed in EPSG:32633 (a projected metric CRS) with 1 m
 *  pixel size. That gives an exact 45-degree slope across the gradient
 *  direction, and an east-facing aspect (~90 deg compass) over most of the
 *  surface.
 */
class DEMProcessingTest extends AnyFunSuite with BeforeAndAfterAll {

    private var demDs: Dataset = _
    private var resultsBuf: List[Dataset] = List.empty

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
        demDs = buildSyntheticDEM(width = 100, height = 100)
    }

    override def afterAll(): Unit = {
        resultsBuf.foreach { d => try d.delete() catch { case _: Throwable => () } }
        if (demDs != null) demDs.delete()
    }

    /** Helper: track result Datasets so we can release them in afterAll. */
    private def track(t: (Dataset, Map[String, String])): (Dataset, Map[String, String]) = {
        resultsBuf = t._1 :: resultsBuf
        t
    }

    /** Build a 100x100 Float32 DEM: west-to-east ramp 0 .. width-1 m, 1 m pixel. */
    private def buildSyntheticDEM(width: Int, height: Int): Dataset = {
        val memDriver = gdal.GetDriverByName("GTiff")
        val path = s"/vsimem/dem_test_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val ds = memDriver.Create(path, width, height, 1, gdalconstConstants.GDT_Float32)
        // EPSG:32633 — UTM zone 33N — projected, units metres.
        ds.SetProjection(srsWkt(32633))
        // Origin at (500000, 5000000); 1 m pixel size (positive E, negative N).
        ds.SetGeoTransform(Array(500000.0, 1.0, 0.0, 5000000.0, 0.0, -1.0))
        val band = ds.GetRasterBand(1)
        // Ramp: each column gets value = column index (0 .. width-1).
        val buf = new Array[Float](width * height)
        var r = 0
        while (r < height) {
            var c = 0
            while (c < width) {
                buf(r * width + c) = c.toFloat
                c += 1
            }
            r += 1
        }
        band.WriteRaster(0, 0, width, height, buf)
        band.FlushCache()
        ds.FlushCache()
        ds
    }

    /** Make an EPSG WKT (lazy, via SpatialReference). */
    private def srsWkt(epsg: Int): String = {
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(epsg)
        val wkt = sr.ExportToWkt()
        sr.delete()
        wkt
    }

    /** Read center pixel of band as Double. */
    private def centerPixel(ds: Dataset, band: Int = 1): Double = {
        val w = ds.GetRasterXSize
        val h = ds.GetRasterYSize
        val buf = new Array[Double](1)
        ds.GetRasterBand(band).ReadRaster(w / 2, h / 2, 1, 1, buf)
        buf(0)
    }

    // ------------------------------------------------------------------
    // Helper-level tests (Task 4 budget: 2-3 tests on the shared helper)
    // ------------------------------------------------------------------

    test("RST_DEMProcessingHelper.process rejects null Dataset and empty processing mode") {
        an[IllegalArgumentException] should be thrownBy {
            RST_DEMProcessingHelper.process(null, "slope")
        }
        an[IllegalArgumentException] should be thrownBy {
            RST_DEMProcessingHelper.process(demDs, "")
        }
    }

    test("RST_DEMProcessingHelper.process returns a GTiff Dataset with the expected metadata stamp") {
        val (out, mtd) = track(RST_DEMProcessingHelper.process(demDs, "TRI"))
        out should not be null
        out.GetDriver().getShortName shouldBe "GTiff"
        mtd("driver") shouldBe "GTiff"
        mtd("extension") shouldBe "tif"
        mtd("format") shouldBe "GTiff"
        mtd("path") should startWith("/vsimem/dem_")
        mtd("last_command") should include("TRI")
    }

    // ------------------------------------------------------------------
    // The happy-path test for the one expression with a JVM form
    // ------------------------------------------------------------------

    test("RST_TRI.execute returns a finite, non-negative ruggedness value on the ramp") {
        val (out, _) = track(RST_TRI.execute(demDs))
        out should not be null
        val v = centerPixel(out)
        v.isNaN shouldBe false
        v should be >= 0.0
    }

}
