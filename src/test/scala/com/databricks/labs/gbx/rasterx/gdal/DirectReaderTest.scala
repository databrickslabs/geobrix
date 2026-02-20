package com.databricks.labs.gbx.rasterx.gdal

import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for DirectReader (readWindow with Band). Requires GDAL (e.g. run in Docker). */
class DirectReaderTest extends AnyFunSuite with BeforeAndAfterAll {

    var memDs: Dataset = _
    var band: org.gdal.gdal.Band = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        val driver = gdal.GetDriverByName("MEM")
        memDs = driver.Create("/vsimem/directreader_test", 10, 10, 1, gdalconstConstants.GDT_Float64)
        band = memDs.GetRasterBand(1)
        band.SetNoDataValue(-9999.0)
        val buf = Array.fill(100)(5.0)
        band.WriteRaster(0, 0, 10, 10, buf)
        band.FlushCache()
    }

    override def afterAll(): Unit = {
        if (memDs != null) memDs.delete()
    }

    test("readWindow with zero width should return null") {
        val reader = new DirectReader(0)
        reader.readWindow(band, (0, 0, 0, 5)) shouldBe null
    }

    test("readWindow with zero height should return null") {
        val reader = new DirectReader(0)
        reader.readWindow(band, (0, 0, 5, 0)) shouldBe null
    }

    test("readWindow (0,0,2,2) should return 2x2 array when band has data") {
        val reader = new DirectReader(0)
        val out = reader.readWindow(band, (0, 0, 2, 2))
        out should not be null
        out.length shouldBe 2
        out(0).length shouldBe 2
        out(0)(0) shouldBe 5.0
        out(1)(1) shouldBe 5.0
    }

    test("readWindow should normalize window (min and abs for w/h)") {
        val reader = new DirectReader(0)
        val out = reader.readWindow(band, (2, 2, 0, 0))
        out should not be null
        out.length shouldBe 2
        out(0).length shouldBe 2
    }

    test("DirectReader with initialCapacity should grow buffers for larger window") {
        val reader = new DirectReader(4)
        val out = reader.readWindow(band, (0, 0, 5, 5))
        out should not be null
        out.length shouldBe 5
        out(0).length shouldBe 5
    }
}
