package com.databricks.labs.gbx.rasterx.gdal

import org.gdal.gdalconst.gdalconstConstants._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GDALUtilsTest extends AnyFunSuite {

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

    test("GDAL fromWorldCoord should floor a point just outside the origin edge") {
        val gt = Array(0.0, 1.0, 0.0, 0.0, 0.0, 1.0)
        val (x, y) = GDAL.fromWorldCoord(gt, -0.5, -0.5)
        x shouldBe -1
        y shouldBe -1
    }

    test("GDAL fromWorldCoord should give the pixel whose centre it is given") {
        val gt = Array(50.0, 0.5, 0.0, 100.0, 0.0, 0.5)
        // the centre of pixel (15, 25)
        val (xGeo, yGeo) = (50.0 + 15.5 * 0.5, 100.0 + 25.5 * 0.5)
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
