package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for RST_Convolve (execute, type branches via Convolve, error path). Requires GDAL (e.g. run in Docker). */
class RST_ConvolveTest extends AnyFunSuite with BeforeAndAfterAll {

    var memDs: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        val driver = gdal.GetDriverByName("MEM")
        memDs = driver.Create("/vsimem/convolve_test", 8, 8, 1, gdalconstConstants.GDT_Float64)
        val band = memDs.GetRasterBand(1)
        val buf = Array.tabulate(64)(i => (i % 8).toDouble)
        band.WriteRaster(0, 0, 8, 8, buf)
        band.FlushCache()
    }

    override def afterAll(): Unit = {
        if (memDs != null) RasterDriver.releaseDataset(memDs)
    }

    test("RST_Convolve.name should be gbx_rst_convolve") {
        RST_Convolve.name shouldBe "gbx_rst_convolve"
    }

    test("RST_Convolve.builder should return RST_Convolve with three children (tile, kernel, config)") {
        import org.apache.spark.sql.catalyst.expressions.Literal
        import org.apache.spark.sql.types.StringType
        val tileLit = Literal.create("", StringType)
        val kernelLit = Literal.create(null, org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.DoubleType)))
        val conv = RST_Convolve.builder()(Seq(tileLit, kernelLit))
        conv should not be null
        conv shouldBe a[RST_Convolve]
        conv.children should have size 3
    }

    test("RST_Convolve.execute with odd kernel should return convolved dataset") {
        val kernel = Array(
            Array(0.0, 1.0, 0.0),
            Array(1.0, -4.0, 1.0),
            Array(0.0, 1.0, 0.0)
        )
        val (resultDs, meta) = RST_Convolve.execute((1L, memDs, Map.empty), kernel)
        resultDs should not be null
        meta should contain key "path"
        resultDs.getRasterXSize shouldBe memDs.getRasterXSize
        resultDs.getRasterYSize shouldBe memDs.getRasterYSize
        resultDs.GetRasterCount shouldBe memDs.GetRasterCount
        RasterDriver.releaseDataset(resultDs)
    }

    test("RST_Convolve.execute with even-sized kernel should throw") {
        val badKernel = Array(Array(1.0, 0.0), Array(0.0, 1.0))
        assertThrows[IllegalArgumentException] {
            RST_Convolve.execute((1L, memDs, Map.empty), badKernel)
        }
    }
}
