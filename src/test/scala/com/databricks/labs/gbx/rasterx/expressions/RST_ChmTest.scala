package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/**
  * Direct-execute unit tests for `RST_Chm.execute`. No Spark session or JAR required.
  * Synthetic rasters are built via the GDAL Java bindings so values are deterministic.
  */
class RST_ChmTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    /** Create a single-band Float32 GeoTIFF at `path` with the given pixel values. */
    private def makeTif(
        path: String,
        vals: Array[Float],
        w: Int,
        h: Int,
        originX: Double,
        originY: Double,
        px: Double,
        nodata: Option[Double]
    ): Unit = {
        val drv = gdal.GetDriverByName("GTiff")
        val ds = drv.Create(path, w, h, 1, gdalconstConstants.GDT_Float32)
        ds.SetGeoTransform(Array(originX, px, 0.0, originY, 0.0, -px))
        val sr = new SpatialReference()
        sr.ImportFromEPSG(32630)
        ds.SetProjection(sr.ExportToWkt())
        val band = ds.GetRasterBand(1)
        nodata.foreach(nd => band.SetNoDataValue(nd))
        band.WriteRaster(0, 0, w, h, vals)
        band.FlushCache()
        ds.FlushCache()
        ds.delete()
        sr.delete()
    }

    /** Read all Float32 values from band 1 of a Dataset (row-major). */
    private def readBand(ds: Dataset): Array[Float] = {
        val w = ds.GetRasterXSize
        val h = ds.GetRasterYSize
        val buf = new Array[Float](w * h)
        ds.GetRasterBand(1).ReadRaster(0, 0, w, h, buf)
        buf
    }

    test("RST_Chm: aligned same-grid CHM = clamp(DSM - DEM, 0)") {
        // DSM [[10,20],[5,8]] - DEM [[10,15],[7,8]] = [[0,5],[-2,0]] -> clamp [[0,5],[0,0]]
        makeTif("/tmp/chm_dsm.tif", Array(10f, 20f, 5f, 8f), 2, 2, 10.0, 50.0, 1.0, Some(-9999.0))
        makeTif("/tmp/chm_dem.tif", Array(10f, 15f, 7f, 8f), 2, 2, 10.0, 50.0, 1.0, Some(-9999.0))
        val dsm = gdal.Open("/tmp/chm_dsm.tif")
        val dem = gdal.Open("/tmp/chm_dem.tif")
        val (res, _) = RST_Chm.execute(dsm, dem, Map.empty)
        res.GetRasterBand(1).getDataType shouldBe gdalconstConstants.GDT_Float32
        val ndArr = new Array[java.lang.Double](1)
        res.GetRasterBand(1).GetNoDataValue(ndArr)
        ndArr(0).doubleValue shouldBe -9999.0
        readBand(res).toSeq shouldBe Seq(0f, 5f, 0f, 0f)
        RasterDriver.releaseDataset(res)
        dsm.delete()
        dem.delete()
    }

    test("RST_Chm: NoData in either input propagates to -9999") {
        makeTif("/tmp/chm_dsm2.tif", Array(10f, -9999f, 5f, 8f), 2, 2, 10.0, 50.0, 1.0, Some(-9999.0))
        makeTif("/tmp/chm_dem2.tif", Array(10f, 15f, 7f, 8f), 2, 2, 10.0, 50.0, 1.0, Some(-9999.0))
        val dsm = gdal.Open("/tmp/chm_dsm2.tif")
        val dem = gdal.Open("/tmp/chm_dem2.tif")
        val (res, _) = RST_Chm.execute(dsm, dem, Map.empty)
        readBand(res)(1) shouldBe -9999f  // masked, not clamp(0-15,0)=0
        readBand(res)(0) shouldBe 0f      // 10-10=0
        RasterDriver.releaseDataset(res)
        dsm.delete()
        dem.delete()
    }

    test("RST_Chm: nodata-less DSM smaller than DEM -> padding is NoData, no canopy below datum") {
        // DEM 2x2 all -20 (below datum), fully valid; DSM 1x1 (top-left) = 5, NO nodata.
        makeTif("/tmp/chm_dem3.tif", Array(-20f, -20f, -20f, -20f), 2, 2, 10.0, 50.0, 1.0, Some(-9999.0))
        makeTif("/tmp/chm_dsm3.tif", Array(5f), 1, 1, 10.0, 50.0, 1.0, None)
        val dsm = gdal.Open("/tmp/chm_dsm3.tif")
        val dem = gdal.Open("/tmp/chm_dem3.tif")
        val (res, _) = RST_Chm.execute(dsm, dem, Map.empty)
        val out = readBand(res)
        out(0) shouldBe 25f     // covered: clamp(5-(-20),0)=25
        out(3) shouldBe -9999f  // uncovered padding masked, NOT clamp(0-(-20),0)=20
        RasterDriver.releaseDataset(res)
        dsm.delete()
        dem.delete()
    }
}
