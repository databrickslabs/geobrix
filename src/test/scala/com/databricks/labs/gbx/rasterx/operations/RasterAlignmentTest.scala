package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
  * The grid-alignment guard the cross-raster combine reducer (`gbx_rst_combineavg_agg`, through [[CombineAVG]]) runs
  * first: inputs on different grids raise a clear error that points at `rst_align_to`.
  */
class RasterAlignmentTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
    }

    /** Square Float64 /vsimem GTiff (georeferenced), NoData=-9999. */
    private def tile(values: Array[Double], epsg: Int = 4326, size: Int = 2): Dataset = {
        val path = s"/vsimem/align_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val d = gdal.GetDriverByName("GTiff").Create(path, size, size, 1, gdalconstConstants.GDT_Float64)
        d.SetGeoTransform(Array(0.0, 1.0, 0.0, size.toDouble, 0.0, -1.0))
        val srs = new org.gdal.osr.SpatialReference(); srs.ImportFromEPSG(epsg)
        d.SetProjection(srs.ExportToWkt()); srs.delete()
        val band = d.GetRasterBand(1)
        band.SetNoDataValue(-9999.0)
        band.WriteRaster(0, 0, size, size, values)
        band.FlushCache(); d.FlushCache(); band.delete()
        d
    }

    test("misaligned inputs (different dimensions) raise a clear error pointing at rst_align_to") {
        val a = tile(Array(1.0, 2.0, 3.0, 4.0))
        val b = tile(Array.fill[Double](9)(1.0), size = 3)
        try {
            val ex = intercept[IllegalArgumentException] {
                CombineAVG.compute(Array(a, b), Map.empty)
            }
            ex.getMessage.toLowerCase should include("align")
        } finally {
            RasterDriver.releaseDataset(a); RasterDriver.releaseDataset(b)
        }
    }

    test("misaligned inputs (different CRS) raise a clear error") {
        val a = tile(Array(1.0, 2.0, 3.0, 4.0), epsg = 4326)
        val b = tile(Array(1.0, 2.0, 3.0, 4.0), epsg = 3857)
        try {
            val ex = intercept[IllegalArgumentException] {
                CombineAVG.compute(Array(a, b), Map.empty)
            }
            ex.getMessage.toLowerCase should include("align")
        } finally {
            RasterDriver.releaseDataset(a); RasterDriver.releaseDataset(b)
        }
    }
}
