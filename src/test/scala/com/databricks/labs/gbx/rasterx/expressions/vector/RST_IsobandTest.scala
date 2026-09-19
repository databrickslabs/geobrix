package com.databricks.labs.gbx.rasterx.expressions.vector

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Direct-execute tests for [[RST_Isoband]].
 *
 *  All rasters are synthetic in-memory fixtures built from the GDAL Java bindings
 *  so values are deterministic and no sample data files are needed.
 */
class RST_IsobandTest extends AnyFunSuite with BeforeAndAfterAll {

    // 4x4 EPSG:4326, left 2 cols = 10.0, right 2 cols = 60.0. No NoData.
    // breaks [0,50,100] -> band 0 [0,50) = 10.0 region, band 1 [50,100) = 60.0 region.
    private var srcDs: Dataset = _

    // 4x4 EPSG:4326 with NoData and out-of-range values:
    //   col 0-1 = -9999.0 (NoData), col 2 = 500.0 (above breaks[-1]=200), col 3 = 10.0 (valid)
    //   breaks [0,100,200] -> only col 3 (10.0) lands in band 0 [0,100)
    private var srcDsNoData: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        Files.createDirectories(NodeFilePathUtil.rootPath)

        val drv = gdal.GetDriverByName("MEM")

        // Two-region raster: left half 10.0, right half 60.0, no NoData.
        srcDs = drv.Create("", 4, 4, 1, gdalconstConstants.GDT_Float64)
        srcDs.SetGeoTransform(Array(0.0, 1.0, 0.0, 4.0, 0.0, -1.0))
        val sr = new SpatialReference()
        sr.ImportFromEPSG(4326)
        srcDs.SetProjection(sr.ExportToWkt())
        sr.delete()
        val band = srcDs.GetRasterBand(1)
        val pixels = (0 until 16).map { i => if ((i % 4) < 2) 10.0 else 60.0 }.toArray
        band.WriteRaster(0, 0, 4, 4, pixels)
        band.FlushCache()

        // NoData + out-of-range raster.
        srcDsNoData = drv.Create("", 4, 4, 1, gdalconstConstants.GDT_Float64)
        srcDsNoData.SetGeoTransform(Array(0.0, 1.0, 0.0, 4.0, 0.0, -1.0))
        val sr2 = new SpatialReference()
        sr2.ImportFromEPSG(4326)
        srcDsNoData.SetProjection(sr2.ExportToWkt())
        sr2.delete()
        val band2 = srcDsNoData.GetRasterBand(1)
        band2.SetNoDataValue(-9999.0)
        val pixels2 = (0 until 16).map { i =>
            val col = i % 4
            col match {
                case 0 | 1 => -9999.0  // NoData -- excluded by mask
                case 2     => 500.0    // above breaks[-1]=200 -- excluded as out-of-range
                case _     => 10.0     // valid, lands in band 0 [0,100)
            }
        }.toArray
        band2.WriteRaster(0, 0, 4, 4, pixels2)
        band2.FlushCache()
    }

    override def afterAll(): Unit = {
        if (srcDs != null) srcDs.delete()
        if (srcDsNoData != null) srcDsNoData.delete()
    }

    test("RST_Isoband: two-region raster produces two patches with correct band/lower/upper and non-empty WKB") {
        val res = RST_Isoband.execute(srcDs, Array(0.0, 50.0, 100.0), 1)
        res should not be null
        val n = res.numElements()
        n shouldBe 2

        val structs = (0 until n).map(i => res.getStruct(i, 4))
        val byBand  = structs.groupBy(_.getInt(1))

        byBand.keySet shouldBe Set(0, 1)
        byBand(0).size shouldBe 1
        byBand(1).size shouldBe 1

        byBand(0).head.getDouble(2) shouldBe 0.0
        byBand(0).head.getDouble(3) shouldBe 50.0
        byBand(1).head.getDouble(2) shouldBe 50.0
        byBand(1).head.getDouble(3) shouldBe 100.0

        structs.foreach { s =>
            val wkb = s.getBinary(0)
            wkb should not be null
            wkb.length should be > 0
        }
    }

    test("RST_Isoband: NoData pixels and out-of-range values produce no struct") {
        // Only col 3 (value 10.0) is in-range; cols 0-1 are NoData, col 2 is 500.0 (>= 200.0).
        val res = RST_Isoband.execute(srcDsNoData, Array(0.0, 100.0, 200.0), 1)
        res should not be null
        val n = res.numElements()
        n shouldBe 1   // exactly one contiguous strip of valid pixels (col 3, all 4 rows)

        val s = res.getStruct(0, 4)
        s.getInt(1) shouldBe 0        // band index 0 = [0,100)
        s.getDouble(2) shouldBe 0.0   // lower
        s.getDouble(3) shouldBe 100.0 // upper

        val wkb = s.getBinary(0)
        wkb should not be null
        wkb.length should be > 0
    }

    test("RST_Isoband: pixel value exactly equal to last break is excluded (upper boundary)") {
        // A value == breaks[-1] must be dropped: binIdx = count(breaks <= v) - 1 = len - 1,
        // which exceeds breaks.length - 2, so it falls outside the valid range.
        // Build a 2x1 raster: left pixel = 50.0 (in band 1 [50,100)), right = 100.0 (== last break -> excluded).
        val drv = gdal.GetDriverByName("MEM")
        val ds  = drv.Create("", 2, 1, 1, gdalconstConstants.GDT_Float64)
        ds.SetGeoTransform(Array(0.0, 1.0, 0.0, 1.0, 0.0, -1.0))
        val sr = new SpatialReference(); sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt()); sr.delete()
        ds.GetRasterBand(1).WriteRaster(0, 0, 2, 1, Array(50.0, 100.0))
        ds.GetRasterBand(1).FlushCache()

        try {
            val res = RST_Isoband.execute(ds, Array(0.0, 50.0, 100.0), 1)
            res should not be null
            val n = res.numElements()
            // Only the 50.0 pixel lands in band 1 [50,100); 100.0 == breaks[-1] is excluded.
            n shouldBe 1
            val s = res.getStruct(0, 4)
            s.getInt(1) shouldBe 1         // band 1
            s.getDouble(2) shouldBe 50.0   // lower
            s.getDouble(3) shouldBe 100.0  // upper
        } finally {
            ds.delete()
        }
    }

    test("RST_Isoband: non-strictly-ascending breaks throw IllegalArgumentException") {
        an[IllegalArgumentException] should be thrownBy {
            RST_Isoband.execute(srcDs, Array(0.0, 50.0, 50.0), 1)
        }
    }

}
