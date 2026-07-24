package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.RasterTessellate
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class RST_Quadbin_TessellateTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
    }

    // A small EPSG:4326 raster over London (~lon -0.1, lat 51.5), 4x4 pixels at ~0.01 degree
    // resolution, all pixels valid. Built in-memory via the MEM driver (RST_BNG_RasterToGridTest pattern).
    private def london4326Ds = {
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("", 4, 4, 1, gdalconstConstants.GDT_Float64)
        // top-left corner (-0.12, 51.52), pixel size ~0.01 degrees
        ds.SetGeoTransform(Array(-0.12, 0.01, 0.0, 51.52, 0.0, -0.01))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        val vals = (1 to 16).map(_.toDouble).toArray
        ds.GetRasterBand(1).WriteRaster(0, 0, 4, 4, vals)
        ds.FlushCache(); ds
    }

    test("quadbin tessellate covering: yields >=1 real clipped chip with a nonzero cell id") {
        val ds = london4326Ds
        // zoom 12 -> quadbin tiles ~0.088 deg wide near this latitude; the ~0.04 deg raster
        // overlaps at least one tile.
        val it = RasterTessellate.tessellateQuadbinIter(ds, Map.empty[String, String], resolution = 12, mode = "covering")
        var count = 0
        while (it.hasNext) {
            val (cell, chip, _) = it.next()
            assert(cell != 0L, "emitted quadbin cell id must be nonzero")
            assert(chip != null, "chip Dataset must not be null")
            assert(chip.getRasterXSize > 0 && chip.getRasterYSize > 0, "chip must be a real clipped tile")
            RasterDriver.releaseDataset(chip)
            count += 1
        }
        it.asInstanceOf[AutoCloseable].close()
        RasterDriver.releaseDataset(ds)
        assert(count >= 1, "covering tessellation must yield at least one chip")
    }

    test("quadbin tessellate: unknown mode throws IllegalArgumentException mentioning 'mode'") {
        val ds = london4326Ds
        val ex = intercept[IllegalArgumentException] {
            RasterTessellate.tessellateQuadbinIter(ds, Map.empty[String, String], resolution = 12, mode = "bogus")
        }
        assert(ex.getMessage.contains("mode"))
        RasterDriver.releaseDataset(ds)
    }
}
