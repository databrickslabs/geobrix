package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.RasterTessellate
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class RST_BNG_TessellateTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
    }

    // A small EPSG:27700 raster over London (BNG easting 530000, northing 180000 -> TQ region),
    // 4x4 pixels at 1km resolution, all pixels valid. Top-left corner (528000, 182000), so the
    // raster spans easting 528000..532000, northing 178000..182000 (4km x 4km). MEM driver.
    private def londonBngDs = {
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("", 4, 4, 1, gdalconstConstants.GDT_Float64)
        ds.SetGeoTransform(Array(528000.0, 1000.0, 0.0, 182000.0, 0.0, -1000.0))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(27700)
        sr.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        ds.SetProjection(sr.ExportToWkt())
        val vals = (1 to 16).map(_.toDouble).toArray
        ds.GetRasterBand(1).WriteRaster(0, 0, 4, 4, vals)
        ds.FlushCache(); ds
    }

    // A small EPSG:4326 raster over London (~lon -0.1, lat 51.5) to exercise the reproject-to-27700
    // warp path. 4x4 pixels at ~0.01 degree, all valid.
    private def london4326Ds = {
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("", 4, 4, 1, gdalconstConstants.GDT_Float64)
        ds.SetGeoTransform(Array(-0.12, 0.01, 0.0, 51.52, 0.0, -0.01))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(4326)
        sr.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        ds.SetProjection(sr.ExportToWkt())
        val vals = (1 to 16).map(_.toDouble).toArray
        ds.GetRasterBand(1).WriteRaster(0, 0, 4, 4, vals)
        ds.FlushCache(); ds
    }

    private val bngCellIdPattern = "^[A-Z]{2}\\d*$".r

    test("bng tessellate covering: yields >=1 areal chip tagged with a BNG string cell id") {
        val ds = londonBngDs
        // resolution 3 = 1km cells; the 4km x 4km raster overlaps multiple 1km cells.
        val it = RasterTessellate.tessellateBngIter(ds, Map.empty[String, String], resolution = 3, mode = "covering")
        var count = 0
        while (it.hasNext) {
            val (cell, chip, _) = it.next()
            assert(bngCellIdPattern.findFirstIn(cell).isDefined, s"emitted BNG cell id must match ^[A-Z]{2}\\d*$$; got '$cell'")
            assert(chip != null, "chip Dataset must not be null")
            assert(chip.getRasterXSize > 0 && chip.getRasterYSize > 0, "chip must be a real clipped tile")
            // chip metadata carries the BNG string id
            val mdId = chip.GetMetadataItem("RASTERX_CELL_ID")
            assert(mdId == cell, s"RASTERX_CELL_ID metadata ('$mdId') must equal emitted cell id ('$cell')")
            assert(bngCellIdPattern.findFirstIn(mdId).isDefined, s"RASTERX_CELL_ID must be a BNG string id; got '$mdId'")
            RasterDriver.releaseDataset(chip)
            count += 1
        }
        it.asInstanceOf[AutoCloseable].close()
        RasterDriver.releaseDataset(ds)
        assert(count >= 1, "covering tessellation must yield at least one chip")
    }

    test("bng tessellate centroid: yields >=1 chip with distinct BNG ids and bounded pixel counts") {
        val ds = londonBngDs
        val srcXSize = ds.getRasterXSize
        val srcYSize = ds.getRasterYSize
        val totalSourcePixels = srcXSize * srcYSize
        val it = RasterTessellate.tessellateBngIter(ds, Map.empty[String, String], resolution = 3, mode = "centroid")
        var count = 0
        var totalAssigned = 0
        val seenIds = scala.collection.mutable.Set[String]()
        while (it.hasNext) {
            val (cell, chip, _) = it.next()
            assert(bngCellIdPattern.findFirstIn(cell).isDefined, s"centroid chip BNG id must match pattern; got '$cell'")
            assert(chip != null, "centroid chip Dataset must not be null")
            seenIds += cell
            val nd = new Array[java.lang.Double](1)
            chip.GetRasterBand(1).GetNoDataValue(nd)
            val buf = new Array[Double](srcXSize * srcYSize)
            chip.GetRasterBand(1).ReadRaster(0, 0, srcXSize, srcYSize, buf)
            val validInChip =
                if (nd(0) != null && nd(0).doubleValue().isNaN) buf.count(!_.isNaN)
                else if (nd(0) != null) buf.count(_ != nd(0).doubleValue())
                else buf.count(!_.isNaN)
            totalAssigned += validInChip
            RasterDriver.releaseDataset(chip)
            count += 1
        }
        it.asInstanceOf[AutoCloseable].close()
        RasterDriver.releaseDataset(ds)
        assert(count >= 1, "centroid tessellation must yield at least one chip")
        assert(seenIds.size == count, "each chip must have a distinct BNG cell id (single-assignment)")
        assert(totalAssigned == totalSourcePixels,
            s"total assigned pixels ($totalAssigned) must equal source pixel count ($totalSourcePixels); pixel drop or duplication detected in centroid mode")
    }

    test("bng tessellate: 4326 input triggers warp and still yields >=1 BNG chip") {
        val ds = london4326Ds
        val it = RasterTessellate.tessellateBngIter(ds, Map.empty[String, String], resolution = 3, mode = "covering")
        var count = 0
        var firstChipSrWkt: String = null
        while (it.hasNext) {
            val (cell, chip, _) = it.next()
            assert(bngCellIdPattern.findFirstIn(cell).isDefined, s"BNG id must match pattern; got '$cell'")
            if (count == 0) firstChipSrWkt = chip.GetProjection()
            RasterDriver.releaseDataset(chip)
            count += 1
        }
        it.asInstanceOf[AutoCloseable].close()
        RasterDriver.releaseDataset(ds)
        assert(count >= 1, "covering tessellation over a 4326 input (post-warp) must yield at least one chip")
        // Verify that the warp actually ran: emitted chips must be in EPSG:27700 (BNG), not the source 4326.
        assert(firstChipSrWkt != null, "expected at least one chip to inspect projection")
        val chipSr = new org.gdal.osr.SpatialReference()
        chipSr.ImportFromWkt(firstChipSrWkt)
        val authorityCode = chipSr.GetAuthorityCode("PROJCS")
        chipSr.delete()
        assert(authorityCode == "27700",
            s"emitted chip must be in EPSG:27700 (warp-to-BNG), but GetAuthorityCode(PROJCS)='$authorityCode'")
    }

    test("bng tessellate: unknown mode throws IllegalArgumentException mentioning 'mode'") {
        val ds = londonBngDs
        val ex = intercept[IllegalArgumentException] {
            RasterTessellate.tessellateBngIter(ds, Map.empty[String, String], resolution = 3, mode = "bogus")
        }
        assert(ex.getMessage.contains("mode"))
        RasterDriver.releaseDataset(ds)
    }
}
