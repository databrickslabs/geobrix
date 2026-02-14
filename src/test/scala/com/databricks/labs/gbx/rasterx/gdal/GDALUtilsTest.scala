package com.databricks.labs.gbx.rasterx.gdal

import org.gdal.gdalconst.gdalconstConstants._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GDALUtilsTest extends AnyFunSuite {

    // ====== Padding ======

    test("Padding case class should store all four sides") {
        val padding = Padding(left = true, right = false, top = true, bottom = false)
        padding.left shouldBe true
        padding.right shouldBe false
        padding.top shouldBe true
        padding.bottom shouldBe false
    }

    test("Padding NoPadding should have all sides false") {
        val noPadding = Padding.NoPadding
        noPadding.left shouldBe false
        noPadding.right shouldBe false
        noPadding.top shouldBe false
        noPadding.bottom shouldBe false
    }

    test("Padding horizontalStrides should return 2 for left and right") {
        val padding = Padding(left = true, right = true, top = false, bottom = false)
        padding.horizontalStrides shouldBe 2
    }

    test("Padding horizontalStrides should return 1 for left only") {
        val padding = Padding(left = true, right = false, top = false, bottom = false)
        padding.horizontalStrides shouldBe 1
    }

    test("Padding horizontalStrides should return 1 for right only") {
        val padding = Padding(left = false, right = true, top = false, bottom = false)
        padding.horizontalStrides shouldBe 1
    }

    test("Padding horizontalStrides should return 0 for none") {
        val padding = Padding.NoPadding
        padding.horizontalStrides shouldBe 0
    }

    test("Padding verticalStrides should return 2 for top and bottom") {
        val padding = Padding(left = false, right = false, top = true, bottom = true)
        padding.verticalStrides shouldBe 2
    }

    test("Padding verticalStrides should return 1 for top only") {
        val padding = Padding(left = false, right = false, top = true, bottom = false)
        padding.verticalStrides shouldBe 1
    }

    test("Padding verticalStrides should return 1 for bottom only") {
        val padding = Padding(left = false, right = false, top = false, bottom = true)
        padding.verticalStrides shouldBe 1
    }

    test("Padding verticalStrides should return 0 for none") {
        val padding = Padding.NoPadding
        padding.verticalStrides shouldBe 0
    }

    test("Padding newOffset should adjust for left padding") {
        val padding = Padding(left = true, right = false, top = false, bottom = false)
        val (x, y) = padding.newOffset(10, 20, 5)
        x shouldBe 15
        y shouldBe 20
    }

    test("Padding newOffset should adjust for top padding") {
        val padding = Padding(left = false, right = false, top = true, bottom = false)
        val (x, y) = padding.newOffset(10, 20, 5)
        x shouldBe 10
        y shouldBe 25
    }

    test("Padding newOffset should adjust for both left and top") {
        val padding = Padding(left = true, right = false, top = true, bottom = false)
        val (x, y) = padding.newOffset(10, 20, 5)
        x shouldBe 15
        y shouldBe 25
    }

    test("Padding newOffset should not adjust with no padding") {
        val padding = Padding.NoPadding
        val (x, y) = padding.newOffset(10, 20, 5)
        x shouldBe 10
        y shouldBe 20
    }

    test("Padding newSize should reduce width for left and right") {
        val padding = Padding(left = true, right = true, top = false, bottom = false)
        val (w, h) = padding.newSize(100, 80, 5)
        w shouldBe 90 // 100 - 2*5
        h shouldBe 80
    }

    test("Padding newSize should reduce width for left only") {
        val padding = Padding(left = true, right = false, top = false, bottom = false)
        val (w, h) = padding.newSize(100, 80, 5)
        w shouldBe 95 // 100 - 5
        h shouldBe 80
    }

    test("Padding newSize should reduce height for top and bottom") {
        val padding = Padding(left = false, right = false, top = true, bottom = true)
        val (w, h) = padding.newSize(100, 80, 5)
        w shouldBe 100
        h shouldBe 70 // 80 - 2*5
    }

    test("Padding newSize should not change size with no padding") {
        val padding = Padding.NoPadding
        val (w, h) = padding.newSize(100, 80, 5)
        w shouldBe 100
        h shouldBe 80
    }

    test("Padding removePadding should remove left padding") {
        val padding = Padding(left = true, right = false, top = false, bottom = false)
        val array = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
        val result = padding.removePadding(array, 3, 1)
        result shouldBe Array(2.0, 3.0, 5.0, 6.0)
    }

    test("Padding removePadding should remove right padding") {
        val padding = Padding(left = false, right = true, top = false, bottom = false)
        val array = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
        val result = padding.removePadding(array, 3, 1)
        result shouldBe Array(1.0, 2.0, 4.0, 5.0)
    }

    test("Padding removePadding should remove top padding") {
        val padding = Padding(left = false, right = false, top = true, bottom = false)
        val array = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
        val result = padding.removePadding(array, 3, 1)
        result shouldBe Array(4.0, 5.0, 6.0)
    }

    test("Padding removePadding should remove bottom padding") {
        val padding = Padding(left = false, right = false, top = false, bottom = true)
        val array = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
        val result = padding.removePadding(array, 3, 1)
        result shouldBe Array(1.0, 2.0, 3.0)
    }

    test("Padding removePadding should not modify with no padding") {
        val padding = Padding.NoPadding
        val array = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
        val result = padding.removePadding(array, 3, 1)
        result shouldBe array
    }

    // ====== FormatLookup ======

    test("FormatLookup should contain GTiff format") {
        FormatLookup.formats should contain key "GTiff"
        FormatLookup.formats("GTiff") shouldBe "tif"
    }

    test("FormatLookup should contain COG format") {
        FormatLookup.formats should contain key "COG"
        FormatLookup.formats("COG") shouldBe "tif"
    }

    test("FormatLookup should contain PNG format") {
        FormatLookup.formats should contain key "PNG"
        FormatLookup.formats("PNG") shouldBe "png"
    }

    test("FormatLookup should contain JPEG format") {
        FormatLookup.formats should contain key "JPEG"
        FormatLookup.formats("JPEG") shouldBe "jpg"
    }

    test("FormatLookup should contain HDF5 format") {
        FormatLookup.formats should contain key "HDF5"
        FormatLookup.formats("HDF5") shouldBe "hdf5"
    }

    test("FormatLookup should contain netCDF format") {
        FormatLookup.formats should contain key "netCDF"
        FormatLookup.formats("netCDF") shouldBe "nc"
    }

    test("FormatLookup should contain Zarr format") {
        FormatLookup.formats should contain key "Zarr"
        FormatLookup.formats("Zarr") shouldBe "zarr"
    }

    test("FormatLookup should contain ESRI Shapefile format") {
        FormatLookup.formats should contain key "ESRI Shapefile"
        FormatLookup.formats("ESRI Shapefile") shouldBe "shp"
    }

    test("FormatLookup should contain VRT format") {
        FormatLookup.formats should contain key "VRT"
        FormatLookup.formats("VRT") shouldBe "vrt"
    }

    test("FormatLookup should contain MEM format") {
        FormatLookup.formats should contain key "MEM"
        FormatLookup.formats("MEM") shouldBe "mem"
    }

    test("FormatLookup should be a non-empty map") {
        FormatLookup.formats should not be empty
        FormatLookup.formats.size should be > 100
    }

    // ====== GDAL Object - Pure Functions ======

    test("GDAL getNoDataConstant should return 0.0 for GDT_Unknown") {
        GDAL.getNoDataConstant(GDT_Unknown) shouldBe 0.0
    }

    test("GDAL getNoDataConstant should return 0.0 for GDT_Byte") {
        GDAL.getNoDataConstant(GDT_Byte) shouldBe 0.0
    }

    test("GDAL getNoDataConstant should return Char.MaxValue for GDT_UInt16") {
        GDAL.getNoDataConstant(GDT_UInt16) shouldBe Char.MaxValue.toDouble
    }

    test("GDAL getNoDataConstant should return Short.MinValue for GDT_Int16") {
        GDAL.getNoDataConstant(GDT_Int16) shouldBe Short.MinValue.toDouble
    }

    test("GDAL getNoDataConstant should return large value for GDT_UInt32") {
        val result = GDAL.getNoDataConstant(GDT_UInt32)
        result shouldBe 2.0 * Int.MaxValue.toDouble
    }

    test("GDAL getNoDataConstant should return Int.MinValue for GDT_Int32") {
        GDAL.getNoDataConstant(GDT_Int32) shouldBe Int.MinValue.toDouble
    }

    test("GDAL getNoDataConstant should return Float.MinValue for GDT_Float32") {
        GDAL.getNoDataConstant(GDT_Float32) shouldBe Float.MinValue.toDouble
    }

    test("GDAL getNoDataConstant should return Double.MinValue for GDT_Float64") {
        GDAL.getNoDataConstant(GDT_Float64) shouldBe Double.MinValue
    }

    test("GDAL toWorldCoord should convert pixel to geographic coordinates") {
        // Simple identity transform with 1-degree resolution
        val gt = Array(0.0, 1.0, 0.0, 0.0, 0.0, 1.0)
        val (xGeo, yGeo) = GDAL.toWorldCoord(gt, 10, 20)
        xGeo shouldBe 10.5 +- 0.01
        yGeo shouldBe 20.5 +- 0.01
    }

    test("GDAL toWorldCoord should handle offset geotransform") {
        // Start at (100, 200) with 0.1-degree resolution
        val gt = Array(100.0, 0.1, 0.0, 200.0, 0.0, 0.1)
        val (xGeo, yGeo) = GDAL.toWorldCoord(gt, 10, 20)
        xGeo shouldBe 101.05 +- 0.01
        yGeo shouldBe 202.05 +- 0.01
    }

    test("GDAL toWorldCoord should handle rotated geotransform") {
        val gt = Array(0.0, 1.0, 0.5, 0.0, 0.5, 1.0)
        val (xGeo, yGeo) = GDAL.toWorldCoord(gt, 10, 20)
        xGeo shouldBe 20.75 +- 0.01 // 0 + (10.5)*1 + (20.5)*0.5
        yGeo shouldBe 25.75 +- 0.01 // 0 + (10.5)*0.5 + (20.5)*1
    }

    test("GDAL fromWorldCoord should convert geographic to pixel coordinates") {
        val gt = Array(0.0, 1.0, 0.0, 0.0, 0.0, 1.0)
        val (x, y) = GDAL.fromWorldCoord(gt, 10.5, 20.5)
        x shouldBe 10
        y shouldBe 20
    }

    test("GDAL fromWorldCoord should handle offset geotransform") {
        val gt = Array(100.0, 0.1, 0.0, 200.0, 0.0, 0.1)
        val (x, y) = GDAL.fromWorldCoord(gt, 101.05, 202.05)
        x shouldBe 10
        y shouldBe 20
    }

    test("GDAL fromWorldCoord should invert toWorldCoord") {
        val gt = Array(50.0, 0.5, 0.0, 100.0, 0.0, 0.5)
        val (xGeo, yGeo) = GDAL.toWorldCoord(gt, 15, 25)
        val (xPix, yPix) = GDAL.fromWorldCoord(gt, xGeo, yGeo)
        xPix shouldBe 15
        yPix shouldBe 25
    }

    test("GDAL WSG84 should be a SpatialReference") {
        GDAL.WSG84 should not be null
        GDAL.WSG84 shouldBe a[org.gdal.osr.SpatialReference]
    }

    test("GDAL EPSG3857 should be a SpatialReference") {
        GDAL.EPSG3857 should not be null
        GDAL.EPSG3857 shouldBe a[org.gdal.osr.SpatialReference]
    }

}
