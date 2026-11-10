package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}

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

    test("RST_AsFormat should convert raster to specified format") {
        val (convertedDs, _) = RST_AsFormat.execute(ds, Map("TILED" -> "YES"), "PNM")
        convertedDs should not be null
        convertedDs.GetDriver().getShortName shouldBe "PNM"
        RasterDriver.releaseDataset(convertedDs)
    }

    test("RST_Convolve should apply convolution kernel to raster") {
        val miniDsPath = "/tmp/mini.tif"
        val (miniDs, _) = GDALTranslate.executeTranslate(miniDsPath, ds, "gdal_translate -outsize 10% 10% -r bilinear", Map.empty)
        val kernel = Array(
          Array(0.0, -1.0, 0.0),
          Array(-1.0, 5.0, -1.0),
          Array(0.0, -1.0, 0.0)
        )
        val (convolvedDs, _) = RST_Convolve.execute((1L, miniDs, Map.empty), kernel)
        convolvedDs should not be null
        convolvedDs.getRasterCount shouldBe miniDs.getRasterCount
        convolvedDs.getRasterXSize shouldBe miniDs.getRasterXSize
        convolvedDs.getRasterYSize shouldBe miniDs.getRasterYSize
        RasterDriver.releaseDataset(convolvedDs)
        RasterDriver.releaseDataset(miniDs)
        Files.deleteIfExists(Paths.get(miniDsPath))
    }

    test("RST_InitNoData should initialize NoData values in raster") {
        val (noDataDs, _) = RST_InitNoData.execute(ds, Map.empty)
        noDataDs should not be null
        noDataDs.GetRasterCount shouldBe ds.GetRasterCount
        noDataDs.getRasterXSize shouldBe ds.getRasterXSize
        noDataDs.getRasterYSize shouldBe ds.getRasterYSize
        noDataDs.delete()
    }

    test("RST_RasterToWorldCoordX should convert raster to world X coordinate") {
        val x = RST_RasterToWorldCoordX.execute(ds, 10000, 10000)
        x should not be 0.0
    }

    test("RST_RasterToWorldCoordY should convert raster to world Y coordinate") {
        val y = RST_RasterToWorldCoordY.execute(ds, 10000, 10000)
        y should not be 0.0
    }

    test("RST_UpdateType should update raster data type") {
        val (updatedDs, _) = RST_UpdateType.execute(ds, Map.empty, "Float32")
        updatedDs should not be null
        updatedDs.GetRasterCount shouldBe ds.GetRasterCount
        updatedDs.getRasterXSize shouldBe ds.getRasterXSize
        updatedDs.getRasterYSize shouldBe ds.getRasterYSize
        updatedDs.delete()
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
