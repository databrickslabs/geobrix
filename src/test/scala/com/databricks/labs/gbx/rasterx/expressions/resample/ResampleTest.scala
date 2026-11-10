package com.databricks.labs.gbx.rasterx.expressions.resample

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Direct-execute tests for the resample-to-size expression + helper.
 *
 *  Each test runs `execute(...)` against a 100x100 synthetic MEM raster — no
 *  Spark session bootstrap, ~1s per test.
 */
class ResampleTest extends AnyFunSuite with BeforeAndAfterAll {

    private var srcDs: Dataset = _
    private var resultsBuf: List[Dataset] = List.empty

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
        srcDs = buildSyntheticRaster(width = 100, height = 100)
    }

    override def afterAll(): Unit = {
        resultsBuf.foreach { d => try d.delete() catch { case _: Throwable => () } }
        if (srcDs != null) srcDs.delete()
    }

    private def track(t: (Dataset, Map[String, String])): (Dataset, Map[String, String]) = {
        resultsBuf = t._1 :: resultsBuf
        t
    }

    /** 100x100 Float32 raster in EPSG:32633 with 10 m pixels, west-to-east ramp 0..99. */
    private def buildSyntheticRaster(width: Int, height: Int): Dataset = {
        val driver = gdal.GetDriverByName("GTiff")
        val path = s"/vsimem/resample_src_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val ds = driver.Create(path, width, height, 1, gdalconstConstants.GDT_Float32)
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(32633)
        ds.SetProjection(sr.ExportToWkt())
        sr.delete()
        // 10 m pixel size, origin at (500000, 5000000). Extent: 100 cols x 10 m = 1000 m wide.
        ds.SetGeoTransform(Array(500000.0, 10.0, 0.0, 5000000.0, 0.0, -10.0))
        val band = ds.GetRasterBand(1)
        val buf = new Array[Float](width * height)
        var r = 0
        while (r < height) {
            var c = 0
            while (c < width) {
                buf(r * width + c) = c.toFloat
                c += 1
            }
            r += 1
        }
        band.WriteRaster(0, 0, width, height, buf)
        band.FlushCache()
        ds.FlushCache()
        ds
    }

    // -------------------------- Helper tests --------------------------------

    test("RST_ResampleHelper rejects unsupported algorithm name") {
        an[IllegalArgumentException] should be thrownBy {
            RST_ResampleHelper.warpToSize(srcDs, Map.empty, 50, 50, "not-an-algo")
        }
    }

    test("RST_ResampleHelper warpToSize rejects non-positive dimensions") {
        an[IllegalArgumentException] should be thrownBy {
            RST_ResampleHelper.warpToSize(srcDs, Map.empty, 0, 50, "near")
        }
        an[IllegalArgumentException] should be thrownBy {
            RST_ResampleHelper.warpToSize(srcDs, Map.empty, 50, -1, "near")
        }
    }

    // ----------------------- Per-expression tests ---------------------------

    test("RST_ResampleToSize produces exactly width_px x height_px (near)") {
        val (out, _) = track(RST_ResampleToSize.execute(srcDs, Map.empty, 50, 50, "near"))
        out should not be null
        out.GetRasterXSize shouldBe 50
        out.GetRasterYSize shouldBe 50
    }

}
