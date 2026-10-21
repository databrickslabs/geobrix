package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.rasterx.expressions.generators.{RST_BNG_Tessellate, RST_Quadbin_Tessellate}
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
        val it = RasterTessellate.tessellateQuadbinIter(ds, Map.empty[String, String], resolution = 12, assignment = "covering")
        var count = 0
        while (it.hasNext) {
            val (cell, chip, _) = it.next()
            assert(cell != 0L, "emitted quadbin cell id must be nonzero")
            assert(chip != null, "chip Dataset must not be null")
            assert(chip.getRasterXSize > 0 && chip.getRasterYSize > 0,
                "chip must be a real clipped tile (Finding 3: clip produced non-empty result)")
            RasterDriver.releaseDataset(chip)
            count += 1
        }
        it.asInstanceOf[AutoCloseable].close()
        assert(count >= 1, "covering tessellation must yield at least one chip")
    }

    test("quadbin tessellate centroid+sparse: yields >=1 chip with nonzero cell ids, ids distinct, pixel count bounded") {
        val ds = london4326Ds
        // zoom 12: the 4x4 (~0.04 deg) raster spans multiple quadbin tiles at this zoom,
        // so centroid mode assigns each pixel to exactly one cell -> multiple chips expected.
        val srcXSize = ds.getRasterXSize
        val srcYSize = ds.getRasterYSize
        val totalSourcePixels = srcXSize * srcYSize // 16 pixels total in fixture
        val srcGt = ds.GetGeoTransform
        val srcVals = new Array[Double](totalSourcePixels)
        ds.GetRasterBand(1).ReadRaster(0, 0, srcXSize, srcYSize, srcVals)
        val it = RasterTessellate.tessellateQuadbinIter(ds, Map.empty[String, String], resolution = 12, assignment = "centroid", coverage = "sparse")
        var count = 0
        var totalAssigned = 0
        val seenIds = scala.collection.mutable.Set[Long]()
        while (it.hasNext) {
            val (cell, chip, _) = it.next()
            assert(cell != 0L, s"centroid chip cell id must be nonzero")
            assert(chip != null, s"centroid chip Dataset must not be null")
            // A centroid chip is cropped to its cell's pixel window: a sub-window of the source grid,
            // located by its geotransform origin.
            val (w, h) = (chip.getRasterXSize, chip.getRasterYSize)
            val cgt = chip.GetGeoTransform
            val xOff = math.round((cgt(0) - srcGt(0)) / srcGt(1)).toInt
            val yOff = math.round((cgt(3) - srcGt(3)) / srcGt(5)).toInt
            assert(w >= 1 && h >= 1 && xOff >= 0 && yOff >= 0 && xOff + w <= srcXSize && yOff + h <= srcYSize,
                s"centroid chip ${w}x$h at ($xOff,$yOff) must be a window of the ${srcXSize}x$srcYSize source")
            seenIds += cell
            // Count assigned (non-nodata) pixels in band 1; each must carry the source value at the
            // chip's offset. The source declares no NoData, so chips blank with the Float64 constant.
            val nd = new Array[java.lang.Double](1)
            chip.GetRasterBand(1).GetNoDataValue(nd)
            assert(nd(0) != null, "centroid chip must declare a NoData value")
            val buf = new Array[Double](w * h)
            chip.GetRasterBand(1).ReadRaster(0, 0, w, h, buf)
            val validIdx = buf.indices.filter(k => buf(k) != nd(0).doubleValue())
            validIdx.foreach { k =>
                val srcIdx = (yOff + k / w) * srcXSize + (xOff + k % w)
                assert(buf(k) == srcVals(srcIdx), s"chip pixel $k must equal source pixel $srcIdx")
            }
            val validInChip = validIdx.length
            totalAssigned += validInChip
            RasterDriver.releaseDataset(chip)
            count += 1
        }
        it.asInstanceOf[AutoCloseable].close()
        assert(count >= 1, "centroid tessellation must yield at least one chip")
        assert(seenIds.size == count, "each chip must have a distinct cell id (single-assignment)")
        assert(totalAssigned <= totalSourcePixels,
            s"total assigned pixels across centroid chips ($totalAssigned) must not exceed source pixel count ($totalSourcePixels)")
    }

    test("quadbin tessellate: unknown assignment throws IllegalArgumentException mentioning 'assignment'") {
        val ds = london4326Ds
        val ex = intercept[IllegalArgumentException] {
            RasterTessellate.tessellateQuadbinIter(ds, Map.empty[String, String], resolution = 12, assignment = "bogus")
        }
        assert(ex.getMessage.contains("assignment"))
    }

    test("quadbin tessellate: unknown coverage throws IllegalArgumentException mentioning 'coverage'") {
        val ds = london4326Ds
        val ex = intercept[IllegalArgumentException] {
            RasterTessellate.tessellateQuadbinIter(ds, Map.empty[String, String], resolution = 12, assignment = "covering", coverage = "bogus")
        }
        assert(ex.getMessage.contains("coverage"))
    }

    test("generator names + default assignment arity") {
        assert(RST_Quadbin_Tessellate.name == "gbx_rst_quadbin_tessellate")
        assert(RST_BNG_Tessellate.name == "gbx_rst_bng_tessellate")
        // 2-arg builder defaults assignment to "centroid", coverage to "complete"
        import org.apache.spark.sql.catalyst.expressions.Literal
        val e = RST_Quadbin_Tessellate.builder()(Seq(Literal("t"), Literal(10)))
        assert(e.isInstanceOf[RST_Quadbin_Tessellate])
        val cast = e.asInstanceOf[RST_Quadbin_Tessellate]
        assert(cast.assignmentExpr == Literal("centroid"), "2-arg default assignment must be 'centroid'")
        assert(cast.coverageExpr == Literal("complete"), "2-arg default coverage must be 'complete'")
    }
}
