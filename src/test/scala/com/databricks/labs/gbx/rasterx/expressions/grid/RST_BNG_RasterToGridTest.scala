package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import org.gdal.gdal.{Dataset, gdal}
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

    /** Reproject `src` to EPSG:27700 with the SAME command the internal auto-warp uses
      * (`gdalwarp -t_srs EPSG:27700 -r near`). Returns a pre-warped Dataset the caller must
      * release. Written to a distinct /vsimem path so it never collides with the internal
      * warp output. */
    private def explicitWarpToBNG(src: Dataset): Dataset = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val driver = src.GetDriver()
        val ext = GDAL.getExtension(driver.getShortName)
        val outPath = s"/vsimem/explicit_bng_$uuid.$ext"
        val (result, _) = GDALWarp.executeWarp(
          outPath,
          Array(src),
          Map.empty[String, String],
          command = "gdalwarp -t_srs EPSG:27700 -r near"
        )
        result
    }

    // Cross-cutting correctness (spec §5): the internal auto-warp (4326 -> 27700) must
    // produce IDENTICAL BNG cell assignments + measures to an explicit upstream gdalwarp
    // using the same nearest-neighbour command. This proves the internal reprojection is
    // not silently perturbing pixel positions or values relative to a user's own warp.
    test("bng rastertogrid: internal warp matches explicit upstream warp (reproject-equivalence)") {
        val ds4326     = london4326Ds                   // execute() triggers the internal warp
        val dsSrc27700 = london4326Ds                   // named handle for the warp source
        val ds27700    = explicitWarpToBNG(dsSrc27700)  // pre-warped; execute() skips its warp
        val meanF = (v: ArrayBuffer[Double]) => v.sum / v.length
        try {
            val a = RST_BNG_RasterToGrid.execute(ds4326, 3, meanF).flatten.toMap  // internal warp path
            val b = RST_BNG_RasterToGrid.execute(ds27700, 3, meanF).flatten.toMap // no-warp path

            assert(a.nonEmpty, "reproject-equivalence fixture must yield at least one BNG cell")
            assert(a.keySet == b.keySet,
              s"internal-warp cell set ${a.keySet} must equal explicit-warp cell set ${b.keySet}")
            a.foreach { case (cell, v) =>
                assert(math.abs(v - b(cell)) < 1e-9,
                  s"cell $cell measure diverged: internal=$v explicit=${b(cell)}")
            }
        } finally {
            RasterDriver.releaseDataset(ds4326)
            RasterDriver.releaseDataset(dsSrc27700)
            RasterDriver.releaseDataset(ds27700)
        }
    }

    test("bng reducer names are canonical") {
        assert(RST_BNG_RasterToGridAvg.name == "gbx_rst_bng_rastertogridavg")
        assert(RST_BNG_RasterToGridCount.name == "gbx_rst_bng_rastertogridcount")
        assert(RST_BNG_RasterToGridMax.name == "gbx_rst_bng_rastertogridmax")
        assert(RST_BNG_RasterToGridMin.name == "gbx_rst_bng_rastertogridmin")
        assert(RST_BNG_RasterToGridMedian.name == "gbx_rst_bng_rastertogridmedian")
    }

    test("bng rastertogrid reducers: min/max/count/median on the london cell") {
        val ds = londonDs
        import scala.collection.mutable.ArrayBuffer
        val minF = (v: ArrayBuffer[Double]) => v.min
        val maxF = (v: ArrayBuffer[Double]) => v.max
        val cntF = (v: ArrayBuffer[Double]) => v.length
        val medF = (v: ArrayBuffer[Double]) => { val s = v.sorted; val m = s.length / 2; if (s.length % 2 == 0) (s(m - 1) + s(m)) / 2.0 else s(m) }
        assert(RST_BNG_RasterToGrid.execute(ds, 3, minF).flatten.head._2 == 1.0)
        assert(RST_BNG_RasterToGrid.execute(ds, 3, maxF).flatten.head._2 == 4.0)
        assert(RST_BNG_RasterToGrid.execute(ds, 3, cntF).flatten.head._2 == 4)
        assert(math.abs(RST_BNG_RasterToGrid.execute(ds, 3, medF).flatten.head._2 - 2.5) < 1e-9)
        RasterDriver.releaseDataset(ds)
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
