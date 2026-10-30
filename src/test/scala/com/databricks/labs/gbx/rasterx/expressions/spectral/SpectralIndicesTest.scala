package com.databricks.labs.gbx.rasterx.expressions.spectral

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}

/**
  * Direct-execute tests for the spectral-index expressions.
  *
  * Each test builds a small (4 x 4) synthetic 4-band Float32 raster with hand-picked
  * constant pixel values per band (band1=red, band2=NIR, band3=blue, band4=SWIR),
  * runs the expression's pure ``execute`` path, then reads the center pixel of the
  * resulting Float32 raster and asserts it matches the hand-computed formula
  * value within ``1e-6``.
  *
  * Each spectral-index expression delegates to ``RST_MapAlgebra``, which shells
  * out to ``gdal_calc.py`` — so each test takes ~1-3 seconds wall-clock, no Spark
  * session required.
  */
class SpectralIndicesTest extends AnyFunSuite with BeforeAndAfterAll {

    // Hand-picked band reflectances. Chosen so the expected output values are
    // exact (or near-exact) decimals.
    //  band 1 = red   = 0.1
    //  band 2 = nir   = 0.4
    //  band 3 = blue  = 0.05
    //  band 4 = swir  = 0.1
    //  band 5 = green = 0.3   (used by NDWI)
    private val RedValue: Float = 0.1f
    private val NirValue: Float = 0.4f
    private val BlueValue: Float = 0.05f
    private val SwirValue: Float = 0.1f
    private val GreenValue: Float = 0.3f

    private val BandRed = 1
    private val BandNir = 2
    private val BandBlue = 3
    private val BandSwir = 4
    private val BandGreen = 5

    private var srcDs: Dataset = _
    private var resultsBuf: List[Dataset] = List.empty
    private var resultPaths: List[String] = List.empty

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        Files.createDirectories(NodeFilePathUtil.rootPath)
        srcDs = buildSyntheticBands(width = 4, height = 4)
    }

    override def afterAll(): Unit = {
        resultsBuf.foreach { d => try d.delete() catch { case _: Throwable => () } }
        resultPaths.foreach { p => try Files.deleteIfExists(Paths.get(p)) catch { case _: Throwable => () } }
        if (srcDs != null) srcDs.delete()
    }

    /** Track result Datasets + their on-disk paths so afterAll can release/delete them. */
    private def track(t: (Dataset, Map[String, String])): (Dataset, Map[String, String]) = {
        resultsBuf = t._1 :: resultsBuf
        val p = t._1.GetDescription()
        if (p != null && !p.startsWith("/vsimem/")) resultPaths = p :: resultPaths
        t
    }

    /**
      * Build a small 4-x-4 Float32 raster with 5 constant-valued bands wired to
      * (red, nir, blue, swir, green) in 1-based order. Persists to disk so
      * gdal_calc (which doesn't support ``/vsimem/`` sources) can read it.
      */
    private def buildSyntheticBands(width: Int, height: Int): Dataset = {
        val path = s"${NodeFilePathUtil.rootPath}/spectral_test_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val driver = gdal.GetDriverByName("GTiff")
        val ds = driver.Create(path, width, height, 5, gdalconstConstants.GDT_Float32)
        // EPSG:32633 - UTM zone 33N, units metres.
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(32633)
        ds.SetProjection(sr.ExportToWkt())
        sr.delete()
        ds.SetGeoTransform(Array(500000.0, 1.0, 0.0, 5000000.0, 0.0, -1.0))

        val n = width * height
        def fill(bandIdx: Int, value: Float): Unit = {
            val buf = Array.fill[Float](n)(value)
            val band = ds.GetRasterBand(bandIdx)
            band.WriteRaster(0, 0, width, height, buf)
            band.FlushCache()
        }
        fill(BandRed, RedValue)
        fill(BandNir, NirValue)
        fill(BandBlue, BlueValue)
        fill(BandSwir, SwirValue)
        fill(BandGreen, GreenValue)
        ds.FlushCache()
        ds
    }

    /** Read center pixel of band 1 as Double. */
    private def centerPixel(ds: Dataset): Double = {
        val w = ds.GetRasterXSize
        val h = ds.GetRasterYSize
        val buf = new Array[Double](1)
        ds.GetRasterBand(1).ReadRaster(w / 2, h / 2, 1, 1, buf)
        buf(0)
    }

    private val Tol: Double = 1e-6

    // ------------------------------------------------------------------
    // One happy-path test per expression - assertion is the formula value.
    // ------------------------------------------------------------------

    test("RST_EVI.execute returns 2.5*(NIR-Red)/(NIR+6*Red-7.5*Blue+L)") {
        // 2.5 * (0.4 - 0.1) / (0.4 + 6*0.1 - 7.5*0.05 + 1.0) = 2.5*0.3/0.625 = 0.4444...
        val (out, _) = track(RST_EVI.execute(srcDs, BandRed, BandNir, BandBlue,
            l = 1.0, c1 = 6.0, c2 = 7.5, g = 2.5))
        out should not be null
        val expected = 2.5 * (0.4 - 0.1) / (0.4 + 6 * 0.1 - 7.5 * 0.05 + 1.0)
        centerPixel(out) shouldBe (expected +- Tol)
    }

    test("RST_SAVI.execute returns (NIR-Red)/(NIR+Red+L)*(1+L)") {
        // (0.4 - 0.1) / (0.4 + 0.1 + 0.5) * 1.5 = 0.3 / 1.0 * 1.5 = 0.45
        val (out, _) = track(RST_SAVI.execute(srcDs, BandRed, BandNir, l = 0.5))
        out should not be null
        val expected = (0.4 - 0.1) / (0.4 + 0.1 + 0.5) * (1.0 + 0.5)
        centerPixel(out) shouldBe (expected +- Tol)
    }

    test("RST_NDWI.execute returns (Green-NIR)/(Green+NIR)") {
        // (0.3 - 0.4) / (0.3 + 0.4) = -0.142857...
        val (out, _) = track(RST_NDWI.execute(srcDs, BandGreen, BandNir))
        out should not be null
        val expected = (0.3 - 0.4) / (0.3 + 0.4)
        centerPixel(out) shouldBe (expected +- Tol)
    }

    test("RST_NBR.execute returns (NIR-SWIR)/(NIR+SWIR)") {
        // (0.4 - 0.1) / (0.4 + 0.1) = 0.6
        val (out, _) = track(RST_NBR.execute(srcDs, BandNir, BandSwir))
        out should not be null
        val expected = (0.4 - 0.1) / (0.4 + 0.1)
        centerPixel(out) shouldBe (expected +- Tol)
    }

    test("RST_Index.execute dispatches NDVI by name via band_map") {
        // NDVI = (NIR - Red) / (NIR + Red) = (0.4-0.1)/(0.4+0.1) = 0.6
        val (out, _) = track(RST_Index.execute(srcDs, "ndvi",
            Map("red" -> BandRed, "nir" -> BandNir)))
        out should not be null
        val expected = (0.4 - 0.1) / (0.4 + 0.1)
        centerPixel(out) shouldBe (expected +- Tol)
    }

    test("RST_Index.execute validates inputs: unknown formula, missing bands, null/empty args") {
        // unknown formula name -> friendly error listing known ones.
        val unknown = intercept[IllegalArgumentException] {
            RST_Index.execute(srcDs, "bogus", Map("red" -> BandRed, "nir" -> BandNir))
        }
        unknown.getMessage should include("unknown formula")
        unknown.getMessage.toLowerCase should include("ndvi")

        // Missing required band in band_map.
        val missing = intercept[IllegalArgumentException] {
            RST_Index.execute(srcDs, "ndvi", Map("nir" -> BandNir)) // no 'red'
        }
        missing.getMessage should include("red")

        // Empty band_map.
        an[IllegalArgumentException] should be thrownBy {
            RST_Index.execute(srcDs, "ndvi", Map.empty[String, Int])
        }
        // Null formula name.
        an[IllegalArgumentException] should be thrownBy {
            RST_Index.execute(srcDs, null, Map("red" -> BandRed, "nir" -> BandNir))
        }
    }

}
