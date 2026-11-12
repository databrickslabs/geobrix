package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class RST_ExpressionExecuteTest extends AnyFunSuite with BeforeAndAfterAll {

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

    test("RST_RasterToWorldCoordX should convert raster to world X coordinate") {
        val x = RST_RasterToWorldCoordX.execute(ds, 10000, 10000)
        x should not be 0.0
    }

    test("RST_WorldToRasterCoordX should convert world X coordinate to raster") {
        val x = RST_WorldToRasterCoordX.execute(ds, 0.0, 0.0)
        x should be >= 0
    }

    test("RST_WorldToRasterCoordY should convert world Y coordinate to raster") {
        val y = RST_WorldToRasterCoordY.execute(ds, 0.0, 0.0)
        y should be >= 0
    }

}
