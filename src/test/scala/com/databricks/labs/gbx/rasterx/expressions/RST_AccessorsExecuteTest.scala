package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.expressions.accessors.{RST_BoundingBox, RST_Format, RST_GetSubdataset, RST_Height, RST_MemSize, RST_NumBands, RST_PixelHeight, RST_PixelWidth, RST_Rotation, RST_SRID, RST_ScaleX, RST_ScaleY, RST_SkewX, RST_SkewY, RST_UpperLeftX, RST_UpperLeftY, RST_Width}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}

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

    test("RST_Format should return short name of the driver") {
        val format = RST_Format.execute(ds)
        format shouldBe a[String]
        format shouldBe "GTiff"
    }

    test("RST_GetSubdataset should return a subdaset of the raster") {
        val netcdfPath = this.getClass
            .getResource(
              "/binary/netcdf-CMIP5/prAdjust_day_HadGEM2-CC_SMHI-DBSrev930-GFD-1981-2010-postproc_rcp45_r1i1p1_20201201-20201231.nc"
            )
            .toString
            .replace("file:/", "/")
        val netcdf = gdal.Open(netcdfPath)
        val sds = RST_GetSubdataset.execute(netcdf, "prAdjust")
        sds should not be null
        sds.GetRasterBand(1).AsMDArray().GetStatistics().getValid_count should not be 0
        netcdf.delete()
    }

    test("RST_Height should return height of the raster") {
        val height = RST_Height.execute(ds)
        height should be > 0
        height shouldBe ds.GetRasterYSize()
    }

    test("RST_MemSize should return approximated memory storage size") {
        val memSize = RST_MemSize.execute(ds)
        val location = ds.GetDescription()
        memSize should not be 0
        val expected = Files.size(Paths.get(location))
        memSize shouldBe expected
        val cpy = ds.GetDriver().CreateCopy("/vsimem/cpy.tif", ds, 1)
        val expected2 = gdal.GetMemFileBuffer("/vsimem/cpy.tif")
        val memSize2 = RST_MemSize.execute(cpy)
        memSize2 shouldBe expected2.length
        cpy.delete()
        gdal.Unlink("/vsimem/cpy.tif")
    }

    test("RST_NumBands should return the number of bands of the raster") {
        val bandCount = RST_NumBands.execute(ds)
        bandCount should be > 0
        bandCount shouldBe ds.GetRasterCount()
    }

    test("RST_PixelHeight should return the pixel height of the raster") {
        val pxHeight = RST_PixelHeight.execute(ds)
        pxHeight should not be 0.0
        pxHeight shouldBe math.abs(ds.GetGeoTransform()(5))
    }

    test("RST_PixelWidth should return the pixel width of the raster") {
        val pxWidth = RST_PixelWidth.execute(ds)
        pxWidth should not be 0.0
        pxWidth shouldBe ds.GetGeoTransform()(1)
    }

    test("RST_Rotation should return the rotation of the raster" ) {
        val rotation = RST_Rotation.execute(ds)
        rotation shouldBe 0.0
        val gt = ds.GetGeoTransform()
        gt(2) shouldBe 0.0
        gt(4) shouldBe 0.0
    }

    test("RST_ScaleX should return the scale x of the raster") {
        val scaleX = RST_ScaleX.execute(ds)
        scaleX should not be 0.0
        scaleX shouldBe ds.GetGeoTransform()(1)
    }

    test("RST_ScaleY should return the scale y of the raster") {
        val scaleY = RST_ScaleY.execute(ds)
        scaleY should not be 0.0
        scaleY shouldBe ds.GetGeoTransform()(5)
    }

    test("RST_SkewX should return the skew x of the raster") {
        val skewX = RST_SkewX.execute(ds)
        skewX shouldBe 0.0
        val gt = ds.GetGeoTransform()
        gt(2) shouldBe 0.0
    }

    test("RST_SkewY should return the skew y of the raster") {
        val skewY = RST_SkewY.execute(ds)
        skewY shouldBe 0.0
        val gt = ds.GetGeoTransform()
        gt(4) shouldBe 0.0
    }

    test("RST_SRID should return the spatial reference ID of the raster") {
        val srid = RST_SRID.execute(ds)
        // The MODIS fixture's World Sinusoidal: GDAL 3.13 identifies it as ESRI:54008, so its SRID is that ESRI code,
        // as the light tier gives (test_crs_accessors) and rst_srid documents ("an EPSG or ESRI code"). GDAL 3.11 did
        // not identify it and gave 0. A CRS with no authority code still gives 0.
        srid shouldBe 54008
    }

    test("RST_SRID is 0 for a CRS whose authority code is not a number (OGC:CRS84), not an error") {
        val sr = new org.gdal.osr.SpatialReference()
        sr.SetFromUserInput("OGC:CRS84")
        val mem = GDALManager.memDriver().Create("", 2, 2, 1)
        try {
            mem.SetGeoTransform(Array(0.0, 1.0, 0.0, 2.0, 0.0, -1.0))
            mem.SetProjection(sr.ExportToWkt())
            RST_SRID.execute(mem) shouldBe 0
        } finally {
            mem.delete(); sr.delete()
        }
    }

    test("RST_UpperLeftX should return the upper left x coordinate of the raster") {
        val ulx = RST_UpperLeftX.execute(ds)
        ulx shouldBe ds.GetGeoTransform()(0)
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
