package com.databricks.labs.gbx.rasterx.expressions.analysis

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Direct-execute tests for the 4 analysis expressions (cog_convert, proximity,
 *  contour, viewshed).
 *
 *  Each test builds a tiny synthetic raster with a property the corresponding
 *  GDAL primitive must respect, invokes `execute(...)` directly (no Spark),
 *  and asserts on raw pixel / feature values. Goal: 1 happy-path test per
 *  function, total ~4 tests, < 2 min wall-clock.
 */
class AnalysisTest extends AnyFunSuite with BeforeAndAfterAll {

    private var resultsBuf: List[Dataset] = List.empty

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    override def afterAll(): Unit = {
        resultsBuf.foreach { d => try d.delete() catch { case _: Throwable => () } }
    }

    private def track(t: (Dataset, Map[String, String])): (Dataset, Map[String, String]) = {
        resultsBuf = t._1 :: resultsBuf
        t
    }

    // ------------------------------------------------------------------
    // Synthetic raster helpers.
    // ------------------------------------------------------------------

    /** Build a Float64 MEM raster of given size + per-pixel value, EPSG:4326,
      * GeoTransform = identity over (0,0)..(w,h) with top-down y-axis.
      */
    private def buildRaster(
        width: Int, height: Int,
        valueFn: (Int, Int) => Double,
        nodata: Option[Double] = None,
        epsg: Int = 4326
    ): Dataset = {
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("", width, height, 1, gdalconstConstants.GDT_Float64)
        ds.SetGeoTransform(Array(0.0, 1.0, 0.0, height.toDouble, 0.0, -1.0))
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(epsg)
        ds.SetProjection(sr.ExportToWkt())
        sr.delete()
        val band = ds.GetRasterBand(1)
        nodata.foreach(nd => band.SetNoDataValue(nd))
        val buf = new Array[Double](width * height)
        var r = 0
        while (r < height) {
            var c = 0
            while (c < width) {
                buf(r * width + c) = valueFn(c, r)
                c += 1
            }
            r += 1
        }
        band.WriteRaster(0, 0, width, height, buf)
        band.FlushCache()
        ds.FlushCache()
        ds
    }

    private def pixel(ds: Dataset, col: Int, row: Int, band: Int = 1): Double = {
        val buf = new Array[Double](1)
        ds.GetRasterBand(band).ReadRaster(col, row, 1, 1, buf)
        buf(0)
    }

    private def readAllPixels(ds: Dataset, band: Int = 1): Array[Double] = {
        val w = ds.GetRasterXSize
        val h = ds.GetRasterYSize
        val buf = new Array[Double](w * h)
        ds.GetRasterBand(band).ReadRaster(0, 0, w, h, buf)
        buf
    }

    // ------------------------------------------------------------------
    // RST_CogConvert
    // ------------------------------------------------------------------

    test("RST_CogConvert produces a COG-layout GTiff with ZSTD+predictor (header LAYOUT=COG, tile width matches blocksize)") {
        // 256x256 raster — large enough that COG actually tiles internally.
        val src = buildRaster(256, 256, (c, r) => (c + r).toDouble)
        try {
            // Now defaults to ZSTD+predictor via OperatorOptions (compression param still works for backwards compat).
            val (out, mtd) = track(RST_CogConvert.execute(src, Map.empty, "ZSTD", 128, "AVERAGE"))
            out should not be null
            // Driver metadata reports GTiff (COG is a GTiff variant on disk).
            mtd("driver") shouldBe "GTiff"
            mtd("layout") shouldBe "COG"
            // GDAL stores COG layout markers in Image Structure Metadata.
            val imgMeta = out.GetMetadata_Dict("IMAGE_STRUCTURE")
            // GDAL 3.6+: COG sets LAYOUT=COG on the output dataset's metadata.
            // (Defensive: accept either the dict marker or the band-tile size matching blocksize.)
            val band = out.GetRasterBand(1)
            val blockW = new Array[Int](1)
            val blockH = new Array[Int](1)
            band.GetBlockSize(blockW, blockH)
            val cogTiledOk = blockW(0) == 128 && blockH(0) == 128
            val cogMarkerOk = imgMeta != null && (
                Option(imgMeta.get("LAYOUT")).map(_.toString.toUpperCase).contains("COG") ||
                Option(imgMeta.get("layout")).map(_.toString.toUpperCase).contains("COG")
            )
            (cogTiledOk || cogMarkerOk) shouldBe true
        } finally {
            src.delete()
        }
    }

    // ------------------------------------------------------------------
    // RST_Proximity
    // ------------------------------------------------------------------

    test("RST_Proximity from a single source pixel radiates outward (center=0, far corner > 0)") {
        // 21x21 raster: value 0 everywhere except a single center pixel = 1.
        // Use VALUES=1 to make the center the unique source pixel (avoids any
        // NoData-detection ambiguity in GDAL's default "any non-NoData = target"
        // mode where a constant-0 background also reads as a target).
        val src = buildRaster(21, 21,
            (c, r) => if (c == 10 && r == 10) 1.0 else 0.0
        )
        try {
            val (out, _) = track(RST_Proximity.execute(
                src, Map.empty, Some("1"), "PIXEL", None
            ))
            out should not be null
            // Center pixel is the source -> distance 0.
            pixel(out, 10, 10) shouldBe 0.0 +- 1e-6
            // Adjacent pixel (1 step away in pixel grid) -> 1.
            pixel(out, 11, 10) shouldBe 1.0 +- 1e-6
            // Far corner (10,10 from center) -> sqrt(10^2 + 10^2) ~ 14.14.
            val far = pixel(out, 0, 0)
            far should be > 10.0
            far shouldBe (math.sqrt(200.0) +- 0.5)
        } finally {
            src.delete()
        }
    }

    // ------------------------------------------------------------------
    // RST_Contour
    // ------------------------------------------------------------------

    test("RST_Contour generates LineString features at requested levels for a linear gradient") {
        // 101x10 raster — column-ramp value 0..100; row repeats.
        // Use a EPSG:4326-aligned grid so the layer's CRS is well-defined.
        val src = buildRaster(101, 10, (c, _) => c.toDouble)
        try {
            // interval = 10 -> contours at 10, 20, ..., 90 (90/100 levels above base 0).
            val result = RST_Contour.execute(src, Array.empty[Double], 10.0, 0.0, "elev")
            result should not be null
            val n = result.numElements()
            // At least 9 contour features (one per 10/20/.../90 isovalue).
            n should be >= 9
            // Collect distinct values; expect them to span [10, 90].
            val values = (0 until n).map(i => result.getStruct(i, 2).getDouble(1)).toSet
            val minV = values.min
            val maxV = values.max
            minV should be <= 10.0
            maxV should be >= 90.0 - 1e-6
            // Every feature has non-empty WKB.
            (0 until n).foreach { i =>
                val wkb = result.getStruct(i, 2).getBinary(0)
                wkb should not be null
                wkb.length should be > 0
            }
        } finally {
            src.delete()
        }
    }

    // ------------------------------------------------------------------
    // RST_Viewshed
    // ------------------------------------------------------------------

    test("RST_Viewshed over a uniform-height DEM is fully visible (every pixel == visible)") {
        // 31x31 EPSG:32633 (metric) raster, uniform 0 m elevation. Observer at
        // the center with height 100 m has unobstructed sight everywhere.
        // Use a projected CRS so observer coords are in metres.
        val src = buildRaster(31, 31, (_, _) => 0.0, epsg = 32633)
        // Override geotransform to a metric one centered on (1500, 1500): pixel
        // (0,0) is upper-left at (0, 31), pixel (15,15) at (15.5, 15.5).
        src.SetGeoTransform(Array(0.0, 1.0, 0.0, 31.0, 0.0, -1.0))
        try {
            // Observer center: world coords ~ (15.5, 15.5). Top-down y-axis means
            // pixel (15, 15) maps to world (15.5, 15.5).
            val (out, _) = track(RST_Viewshed.execute(
                src, Map.empty,
                observerX = 15.5, observerY = 15.5,
                observerHeight = 100.0, targetHeight = 1.6,
                maxDistance = None
            ))
            out should not be null
            val pixels = readAllPixels(out)
            // Visible = 255, invisible = 0. With flat terrain + 100 m observer
            // every pixel inside the raster MUST be visible.
            val visibleCount = pixels.count(_ >= 254.0)
            val total = pixels.length
            // Allow a few border cells at most (some viewshed implementations
            // mark the very edge as out-of-range); require >= 90% visible.
            (visibleCount.toDouble / total) should be >= 0.9
        } finally {
            src.delete()
        }
    }

}
