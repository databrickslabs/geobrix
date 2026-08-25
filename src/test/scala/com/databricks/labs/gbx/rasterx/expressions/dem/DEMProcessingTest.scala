package com.databricks.labs.gbx.rasterx.expressions.dem

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Direct-execute tests for the Wave 8a terrain-analysis expressions.
 *
 *  We exercise each expression's pure compute path (`execute(...)`) on a small
 *  100x100 synthetic DEM. That avoids a full Spark session bootstrap and keeps
 *  each test under ~1s wall-clock.
 *
 *  The synthetic DEM is a linear west-to-east ramp from 0 to 100 m elevation
 *  (1 m per pixel), placed in EPSG:32633 (a projected metric CRS) with 1 m
 *  pixel size. That gives an exact 45-degree slope across the gradient
 *  direction, and an east-facing aspect (~90 deg compass) over most of the
 *  surface.
 */
class DEMProcessingTest extends AnyFunSuite with BeforeAndAfterAll {

    private var demDs: Dataset = _
    private var geoDemDs: Dataset = _
    private var resultsBuf: List[Dataset] = List.empty

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
        demDs = buildSyntheticDEM(width = 100, height = 100)
        geoDemDs = buildGeographicDEM(width = 100, height = 100)
    }

    override def afterAll(): Unit = {
        resultsBuf.foreach { d => try d.delete() catch { case _: Throwable => () } }
        if (demDs != null) demDs.delete()
        if (geoDemDs != null) geoDemDs.delete()
    }

    /** Helper: track result Datasets so we can release them in afterAll. */
    private def track(t: (Dataset, Map[String, String])): (Dataset, Map[String, String]) = {
        resultsBuf = t._1 :: resultsBuf
        t
    }

    /** Build a 100x100 Float32 DEM: west-to-east ramp 0 .. width-1 m, 1 m pixel. */
    private def buildSyntheticDEM(width: Int, height: Int): Dataset = {
        val memDriver = gdal.GetDriverByName("GTiff")
        val path = s"/vsimem/dem_test_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val ds = memDriver.Create(path, width, height, 1, gdalconstConstants.GDT_Float32)
        // EPSG:32633 — UTM zone 33N — projected, units metres.
        ds.SetProjection(srsWkt(32633))
        // Origin at (500000, 5000000); 1 m pixel size (positive E, negative N).
        ds.SetGeoTransform(Array(500000.0, 1.0, 0.0, 5000000.0, 0.0, -1.0))
        val band = ds.GetRasterBand(1)
        // Ramp: each column gets value = column index (0 .. width-1).
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

    /** Build a 100x100 Float32 DEM in a GEOGRAPHIC CRS (EPSG:4326).
     *
     *  Same west-to-east elevation ramp (0 .. width-1 m) but the pixel size is
     *  ~0.001 degrees (≈111 m at the equator) rather than 1 m. On a geographic
     *  raster, `gdaldem slope` must auto-derive a degree→metre scale (GDAL 3.11+)
     *  or the slope saturates at ~90 deg.
     */
    private def buildGeographicDEM(width: Int, height: Int): Dataset = {
        val memDriver = gdal.GetDriverByName("GTiff")
        val path = s"/vsimem/dem_geo_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val ds = memDriver.Create(path, width, height, 1, gdalconstConstants.GDT_Float32)
        // EPSG:4326 — geographic, units degrees.
        ds.SetProjection(srsWkt(4326))
        // Origin at lon=0, lat=0; ~0.001 deg pixel size (≈111 m at equator).
        ds.SetGeoTransform(Array(0.0, 0.001, 0.0, 0.0, 0.0, -0.001))
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

    /** Make an EPSG WKT (lazy, via SpatialReference). */
    private def srsWkt(epsg: Int): String = {
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(epsg)
        val wkt = sr.ExportToWkt()
        sr.delete()
        wkt
    }

    /** Read center pixel of band as Double. */
    private def centerPixel(ds: Dataset, band: Int = 1): Double = {
        val w = ds.GetRasterXSize
        val h = ds.GetRasterYSize
        val buf = new Array[Double](1)
        ds.GetRasterBand(band).ReadRaster(w / 2, h / 2, 1, 1, buf)
        buf(0)
    }

    // ------------------------------------------------------------------
    // Helper-level tests (Task 4 budget: 2-3 tests on the shared helper)
    // ------------------------------------------------------------------

    test("RST_DEMProcessingHelper.process rejects null Dataset and empty processing mode") {
        an[IllegalArgumentException] should be thrownBy {
            RST_DEMProcessingHelper.process(null, "slope")
        }
        an[IllegalArgumentException] should be thrownBy {
            RST_DEMProcessingHelper.process(demDs, "")
        }
    }

    test("RST_DEMProcessingHelper.process returns a GTiff Dataset with the expected metadata stamp") {
        val (out, mtd) = track(RST_DEMProcessingHelper.process(demDs, "Roughness"))
        out should not be null
        out.GetDriver().getShortName shouldBe "GTiff"
        mtd("driver") shouldBe "GTiff"
        mtd("extension") shouldBe "tif"
        mtd("format") shouldBe "GTiff"
        mtd("path") should startWith("/vsimem/dem_")
        mtd("last_command") should include("Roughness")
    }

    // ------------------------------------------------------------------
    // One happy-path test per expression (Task 4 budget: 7 tests)
    // ------------------------------------------------------------------

    test("RST_Slope.execute returns ~45 deg slope across the 1-m-per-pixel east-ramp (isotropic 1.0/1.0)") {
        val (out, _) = track(RST_Slope.execute(demDs, "degrees", 1.0, 1.0))
        out should not be null
        // Tolerance is broad - the center cell of a 1m/m gradient should be ~45 deg.
        val sl = centerPixel(out)
        sl should (be > 30.0 and be < 60.0)
    }

    /** Extract the default xscale/yscale literals the builder injects when the caller
     *  omits them (1-arg form). Both must be NaN (GDAL-normal auto-scale).
     */
    private def builderDefaultScales(): (Double, Double) = {
        val tile = org.apache.spark.sql.catalyst.expressions.Literal(null, org.apache.spark.sql.types.BinaryType)
        val expr = RST_Slope.builder()(Seq(tile)).asInstanceOf[RST_Slope]
        val xs = expr.xscaleExpr.asInstanceOf[org.apache.spark.sql.catalyst.expressions.Literal].value
            .asInstanceOf[Double]
        val ys = expr.yscaleExpr.asInstanceOf[org.apache.spark.sql.catalyst.expressions.Literal].value
            .asInstanceOf[Double]
        (xs, ys)
    }

    test("RST_Slope default (no explicit xscale/yscale) injects NaN sentinels so -xscale/-yscale are omitted") {
        // GDAL-normal default: the 1-arg/2-arg builder must inject (NaN, NaN), not
        // 1.0. NaN sentinels cause omission of -xscale/-yscale, letting GDAL 3.11
        // auto-derive the CRS scale.
        val (xs, ys) = builderDefaultScales()
        xs.isNaN shouldBe true
        ys.isNaN shouldBe true
    }

    test("RST_Slope auto-scales a geographic (EPSG:4326) DEM but saturates with explicit xscale=1.0/yscale=1.0") {
        // Drive the SAME paths the SQL/Column API drives: the default (NaN/NaN)
        // vs an explicit (1.0, 1.0). On a geographic raster, NaN/NaN omits
        // -xscale/-yscale so GDAL auto-derives the scale, while 1.0/1.0 treats
        // ~0.001 deg spacing as metres and saturates near vertical.
        val (xs, ys) = builderDefaultScales()
        val (auto, _) = track(RST_Slope.execute(geoDemDs, "degrees", xs, ys))
        val (saturated, _) = track(RST_Slope.execute(geoDemDs, "degrees", 1.0, 1.0))
        auto should not be null
        saturated should not be null
        val autoSl = centerPixel(auto)
        val satSl = centerPixel(saturated)
        // Saturated (-xscale 1.0 -yscale 1.0) is pinned near vertical; auto is well below it.
        satSl should be > 80.0
        autoSl should be < 80.0
        satSl should be > (autoSl + 1.0)
    }

    test("RST_Slope anisotropic xscale: doubling xscale halves perceived gradient in X axis") {
        // The test DEM is a west-to-east ramp (all gradient is in X). Doubling xscale
        // tells GDAL each pixel spans 2 m in X instead of 1 m, halving the perceived
        // slope steepness in that direction. Verify the result differs from xscale=1.0.
        val (iso, _) = track(RST_Slope.execute(demDs, "degrees", 1.0, 1.0))
        val (aniso, _) = track(RST_Slope.execute(demDs, "degrees", 2.0, 1.0))
        iso should not be null
        aniso should not be null
        val isoSl = centerPixel(iso)
        val anisoSl = centerPixel(aniso)
        // xscale=2.0 makes the pixel 2x wider in X, so the slope angle decreases.
        math.abs(isoSl - anisoSl) should be > 0.1
        anisoSl should be < isoSl
    }

    test("RST_Slope builder rejects arity-3 (xscale without yscale)") {
        val tile = org.apache.spark.sql.catalyst.expressions.Literal(null, org.apache.spark.sql.types.BinaryType)
        val unit = org.apache.spark.sql.catalyst.expressions.Literal(
            org.apache.spark.unsafe.types.UTF8String.fromString("degrees"),
            org.apache.spark.sql.types.StringType
        )
        val scale = org.apache.spark.sql.catalyst.expressions.Literal(1.0, org.apache.spark.sql.types.DoubleType)
        an[IllegalArgumentException] should be thrownBy {
            RST_Slope.builder()(Seq(tile, unit, scale))
        }
    }

    test("RST_Aspect.execute returns ~270 deg (west-facing) for a west-to-east ramp") {
        // A west-to-east-rising ramp slopes UP to the east; gdaldem reports the
        // direction the slope FACES (downhill normal), which is west - ~270 deg
        // on the compass convention.
        val (out, _) = track(RST_Aspect.execute(demDs, trigonometric = false, zeroForFlat = false))
        out should not be null
        val asp = centerPixel(out)
        asp should (be > 240.0 and be < 300.0)
    }

    test("RST_Hillshade.execute returns a Byte band with values in 0..255") {
        val (out, _) = track(RST_Hillshade.execute(demDs, 315.0, 45.0, 1.0))
        out should not be null
        val band = out.GetRasterBand(1)
        band.getDataType shouldBe gdalconstConstants.GDT_Byte
        val hs = centerPixel(out)
        hs should (be >= 0.0 and be <= 255.0)
    }

    test("RST_TRI.execute returns a finite, non-negative ruggedness value on the ramp") {
        val (out, _) = track(RST_TRI.execute(demDs))
        out should not be null
        val v = centerPixel(out)
        v.isNaN shouldBe false
        v should be >= 0.0
    }

    test("RST_TPI.execute returns a finite value (positive or negative) on the ramp") {
        val (out, _) = track(RST_TPI.execute(demDs))
        out should not be null
        val v = centerPixel(out)
        v.isNaN shouldBe false
        // On a perfectly linear ramp the local mean equals the central pixel ->
        // TPI is approximately 0. Just assert finite.
        math.abs(v) should be < 100.0
    }

    test("RST_Roughness.execute returns a positive max-neighbour difference on the ramp") {
        val (out, _) = track(RST_Roughness.execute(demDs))
        out should not be null
        val v = centerPixel(out)
        v.isNaN shouldBe false
        // On a 1-m-per-pixel ramp, the largest inter-cell delta in a 3x3
        // window is 2 (e.g. leftmost vs rightmost column - 2 columns apart).
        // Assert positive but bounded by a sane upper bound.
        v should (be > 0.5 and be <= 2.5)
    }

    test("RST_ColorRelief.execute produces a multi-band RGB(A) image given a color table") {
        // Minimal color table covering the 0..99 elevation range we wrote.
        val ctPath = Files.createTempFile("gbx_dem_color_", ".txt")
        Files.writeString(ctPath,
            """0 0 0 0
              |50 128 128 128
              |99 255 255 255
              |""".stripMargin)
        try {
            val (out, _) = track(RST_ColorRelief.execute(demDs, ctPath.toString))
            out should not be null
            // gdaldem color-relief emits a 3-band (RGB) or 4-band (RGBA) raster.
            val nb = out.GetRasterCount
            (nb == 3 || nb == 4) shouldBe true
            val band = out.GetRasterBand(1)
            band.getDataType shouldBe gdalconstConstants.GDT_Byte
        } finally {
            Files.deleteIfExists(ctPath)
        }
    }

    test("RST_ColorRelief.execute rejects a null or empty color_table_path") {
        an[IllegalArgumentException] should be thrownBy {
            RST_ColorRelief.execute(demDs, null)
        }
        an[IllegalArgumentException] should be thrownBy {
            RST_ColorRelief.execute(demDs, "")
        }
    }

}
