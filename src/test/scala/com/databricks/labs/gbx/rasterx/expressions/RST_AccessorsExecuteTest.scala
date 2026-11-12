package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.expressions.accessors.{RST_BoundingBox, RST_NumBands, RST_UpperLeftY, RST_Width}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class RST_AccessorsExecuteTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString.replace("file:/", "/")
        ds = gdal.Open(tifPath)
    }

    override def afterAll(): Unit = {
        ds.delete()
    }

    test("RST_BoundingBox should return a non empty polygon") {
        val bbox = RST_BoundingBox.execute(ds)
        bbox should not be null
        bbox.isEmpty shouldBe false
    }

    test("RST_NumBands should return the number of bands of the raster") {
        val bandCount = RST_NumBands.execute(ds)
        bandCount should be > 0
        bandCount shouldBe ds.GetRasterCount()
    }

    test("RST_UpperLeftY should return the upper left y coordinate of the raster") {
        val uly = RST_UpperLeftY.execute(ds)
        uly shouldBe ds.GetGeoTransform()(3)
    }

    test("RST_Width should return width of the raster") {
        val width = RST_Width.execute(ds)
        width should be > 0
        width shouldBe ds.GetRasterXSize()
    }

}
