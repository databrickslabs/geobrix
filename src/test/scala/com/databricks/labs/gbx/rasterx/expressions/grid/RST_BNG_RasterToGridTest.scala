package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer

class RST_BNG_RasterToGridTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
    }

    // A 2x2 EPSG:27700 raster centred on London (530000,180000), 100m pixels,
    // all pixels valid, values 1,2,3,4. Built in-memory via MEM driver.
    private def londonDs = {
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("", 2, 2, 1, gdalconstConstants.GDT_Float64)
        ds.SetGeoTransform(Array(530000.0, 100.0, 0.0, 180200.0, 0.0, -100.0))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(27700)
        ds.SetProjection(sr.ExportToWkt())
        ds.GetRasterBand(1).WriteRaster(0, 0, 2, 2, Array(1.0, 2.0, 3.0, 4.0))
        ds.FlushCache(); ds
    }

    test("bng rastertogrid: emits String cell ids and averages valid pixels") {
        val ds = londonDs
        val meanF = (v: ArrayBuffer[Double]) => v.sum / v.length
        val out: Array[Array[(String, Double)]] =
            RST_BNG_RasterToGrid.execute(ds, resolution = 3, fAgg = meanF) // 3 = 1km
        RasterDriver.releaseDataset(ds)
        val cells = out.flatten
        assert(cells.nonEmpty)
        assert(cells.forall(_._1.matches("^[A-Z]{2}\\d*$"))) // BNG string form
        // all four pixels fall in the same 1km cell here -> mean 2.5
        assert(cells.map(_._1).distinct.length == 1)
        assert(math.abs(cells.head._2 - 2.5) < 1e-9)
    }

    test("bng rastertogrid: zero-valid-pixel cell is never emitted (spec 2.6)") {
        // Mask all pixels nodata -> no cells at all.
        val ds = londonDs
        ds.GetRasterBand(1).SetNoDataValue(1.0)
        ds.GetRasterBand(1).Fill(1.0)
        val meanF = (v: ArrayBuffer[Double]) => v.sum / v.length
        val out = RST_BNG_RasterToGrid.execute(ds, 3, meanF)
        RasterDriver.releaseDataset(ds)
        assert(out.flatten.isEmpty, "all-nodata raster must yield no cells")
    }

    // A 2x2 EPSG:4326 raster covering the same London area as londonDs.
    // London ~ lon -0.1, lat 51.5; 2x2 pixels at ~0.001 degree resolution.
    private def london4326Ds = {
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("", 2, 2, 1, gdalconstConstants.GDT_Float64)
        // top-left corner (-0.102, 51.502), pixel size ~0.001 degrees
        ds.SetGeoTransform(Array(-0.102, 0.001, 0.0, 51.502, 0.0, -0.001))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        ds.GetRasterBand(1).WriteRaster(0, 0, 2, 2, Array(10.0, 20.0, 30.0, 40.0))
        ds.FlushCache(); ds
    }

    test("bng rastertogrid: EPSG:4326 input triggers warp to 27700 and returns valid BNG cells") {
        val ds = london4326Ds
        val meanF = (v: ArrayBuffer[Double]) => v.sum / v.length
        val out: Array[Array[(String, Double)]] =
            RST_BNG_RasterToGrid.execute(ds, resolution = 3, fAgg = meanF)
        RasterDriver.releaseDataset(ds)
        val cells = out.flatten
        assert(cells.nonEmpty, "warped EPSG:4326 raster must produce at least one BNG cell")
        assert(cells.forall(_._1.matches("^[A-Z]{2}\\d*$")), "all cell ids must be valid BNG string form")
    }

    test("bng rastertogrid: pixels fully outside GB extent are dropped (Finding A)") {
        // Build a raster straddling the GB eastern boundary: two pixels at easting ~690000
        // (inside GB) and two at easting ~710000 (outside GB, >700000).
        // The in-boundary pixels must appear; the out-of-boundary pixels must be silently dropped.
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("", 4, 1, 1, gdalconstConstants.GDT_Float64)
        // 4 pixels at 100m spacing starting at easting 688000, northing 180100
        ds.SetGeoTransform(Array(688000.0, 5000.0, 0.0, 180100.0, 0.0, -100.0))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(27700)
        ds.SetProjection(sr.ExportToWkt())
        // pixel centroids at eastings: 688000+2500=690500, 693500, 698500, 703500
        // 690500 and 693500 are inside GB (<=700000); 698500 is inside; 703500 is outside
        ds.GetRasterBand(1).WriteRaster(0, 0, 4, 1, Array(1.0, 2.0, 3.0, 99.0))
        ds.FlushCache()
        val meanF = (v: ArrayBuffer[Double]) => v.sum / v.length
        val out = RST_BNG_RasterToGrid.execute(ds, 3, meanF)
        RasterDriver.releaseDataset(ds)
        val cells = out.flatten
        // Out-of-GB pixel (value 99.0) must not appear
        assert(cells.nonEmpty, "in-GB pixels must produce cells")
        assert(cells.forall(_._2 != 99.0), "out-of-GB pixel (value 99.0) must be dropped")
        assert(cells.forall(_._1.matches("^[A-Z]{2}\\d*$")), "all returned cells must be valid BNG string form")
    }
}
