package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.util.Try

class GDALInfoTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").getPath
        ds = gdal.Open(tifPath)
    }

    override def afterAll(): Unit = {
        if (ds != null) ds.delete()
    }

    test("GDALInfo should return raster information") {
        val command = "gdalinfo"
        val info = GDALInfo.executeInfo(ds, command)

        info should not be null
        info should not be empty
        // Info should contain raster details
        info.length should be > 0
    }

    test("GDALInfo should reject invalid command") {
        val invalidCommand = "invalid_command"
        
        assertThrows[IllegalArgumentException] {
            GDALInfo.executeInfo(ds, invalidCommand)
        }
    }

    test("GDALInfo should provide raster details with options") {
        // Test with various valid options
        val command = "gdalinfo -json"
        val info = Try(GDALInfo.executeInfo(ds, command))
        
        // Should either succeed or handle errors gracefully
        info.isSuccess shouldBe true
        info.get should not be empty
    }

}
