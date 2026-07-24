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
}
