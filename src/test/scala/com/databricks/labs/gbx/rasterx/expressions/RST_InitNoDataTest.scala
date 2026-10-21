package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
  * [[RST_InitNoData]] on bands that declare no NoData: the band gets its type's constant
  * (`GDAL.getNoDataConstant`, 0 for uint8), and the source pixels equal to it become NoData, as in the
  * lightweight tier. gdalwarp would otherwise move such a pixel to the constant +/- 1 and keep it valid.
  */
class RST_InitNoDataTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
    }

    private val W = 8
    private val H = 4

    /** In-memory uint8 raster; band b holds `values(b)` (row-major), with `noData(b)` declared if given. */
    private def uint8(values: Seq[Array[Int]], noData: Seq[Option[Double]]): Dataset = {
        val ds = gdal.GetDriverByName("MEM").Create("", W, H, values.size, gdalconstConstants.GDT_Byte)
        ds.SetGeoTransform(Array(0.0, 0.01, 0.0, 1.0, 0.0, -0.01))
        val srs = new org.gdal.osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        srs.delete()
        values.zip(noData).zipWithIndex.foreach { case ((v, nd), i) =>
            val band = ds.GetRasterBand(i + 1)
            band.WriteRaster(0, 0, W, H, v)
            nd.foreach(band.SetNoDataValue)
        }
        ds
    }

    /** Band `b` values and mask (0 = NoData). */
    private def read(ds: Dataset, b: Int): (Array[Int], Array[Byte]) = {
        val vals = new Array[Int](W * H)
        val mask = new Array[Byte](W * H)
        ds.GetRasterBand(b).ReadRaster(0, 0, W, H, vals)
        ds.GetRasterBand(b).GetMaskBand().ReadRaster(0, 0, W, H, mask)
        (vals, mask)
    }

    private def noDataOf(ds: Dataset, b: Int): Option[Double] = {
        val nd = new Array[java.lang.Double](1)
        ds.GetRasterBand(b).GetNoDataValue(nd)
        Option(nd(0)).map(_.doubleValue())
    }

    /** A ramp 1..31 with real 0s in every fourth pixel. */
    private val ramp: Array[Int] = Array.tabulate(W * H)(i => if (i % 4 == 0) 0 else i % 32)

    test("a uint8 band without NoData: NoData 0, and its real 0 pixels become NoData") {
        val src = uint8(Seq(ramp), Seq(None))
        val (out, _) = RST_InitNoData.execute(src, Map.empty)
        try {
            assert(noDataOf(out, 1).contains(0.0))
            val (vals, mask) = read(out, 1)
            val nudged = ramp.indices.filter(i => ramp(i) == 0 && mask(i) != 0)
            assert(nudged.isEmpty, s"real 0 pixels kept valid at ${nudged.take(5)} as ${nudged.take(5).map(vals(_))}")
            val changed = ramp.indices.filter(i => ramp(i) != 0 && (mask(i) == 0 || vals(i) != ramp(i)))
            assert(changed.isEmpty, s"non-zero pixels changed at ${changed.take(5)}")
        } finally {
            RasterDriver.releaseDataset(out)
            src.delete()
        }
    }

    test("mixed bands: an undeclared band blanks its 0s, a declared band keeps its own NoData pixels as NoData") {
        val band2 = Array.tabulate(W * H)(i => if (i % 3 == 0) 7 else 20 + i % 10)
        val src = uint8(Seq(ramp, band2), Seq(None, Some(7.0)))
        val (out, _) = RST_InitNoData.execute(src, Map.empty)
        try {
            val (_, mask1) = read(out, 1)
            val (_, mask2) = read(out, 2)
            val valid1 = ramp.indices.filter(i => ramp(i) == 0 && mask1(i) != 0)
            assert(valid1.isEmpty, s"band 1 real 0 pixels kept valid at ${valid1.take(5)}")
            val valid2 = band2.indices.filter(i => band2(i) == 7 && mask2(i) != 0)
            assert(valid2.isEmpty, s"band 2 declared NoData pixels became valid at ${valid2.take(5)}")
            val lost2 = band2.indices.filter(i => band2(i) != 7 && mask2(i) == 0)
            assert(lost2.isEmpty, s"band 2 valid pixels became NoData at ${lost2.take(5)}")
        } finally {
            RasterDriver.releaseDataset(out)
            src.delete()
        }
    }
}
