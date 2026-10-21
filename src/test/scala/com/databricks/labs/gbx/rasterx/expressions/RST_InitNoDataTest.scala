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
  * A source with its own mask band keeps that mask.
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

    test("a uint16 band without NoData but with its own mask: the masked pixels stay NoData") {
        // One real 65535 (unmasked): with a source NoData set, gdalwarp then drops the source mask.
        val fallbackAt = 7
        val values = Array.tabulate(W * H)(i => if (i == fallbackAt) 65535 else 100 + i)
        // A GTiff with an internal mask: gdalwarp ignores this kind of mask once a source NoData is set.
        val path = s"/vsimem/initnodata_mask_${java.util.UUID.randomUUID()}.tif"
        val made = gdal.GetDriverByName("GTiff").Create(path, W, H, 1, gdalconstConstants.GDT_UInt16)
        made.SetGeoTransform(Array(0.0, 0.01, 0.0, 1.0, 0.0, -0.01))
        val srs = new org.gdal.osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        made.SetProjection(srs.ExportToWkt())
        srs.delete()
        made.GetRasterBand(1).WriteRaster(0, 0, W, H, values)
        made.CreateMaskBand(gdalconstConstants.GMF_PER_DATASET)
        val srcMask = Array.tabulate[Byte](W * H)(i => if (i % 5 == 0) 0.toByte else 255.toByte)
        made.GetRasterBand(1).GetMaskBand().WriteRaster(0, 0, W, H, srcMask)
        made.delete() // reopen read-only, as a tile read from its bytes is
        val src = gdal.Open(path)
        assert(src.GetRasterBand(1).GetMaskFlags() == gdalconstConstants.GMF_PER_DATASET, "fixture needs a per-dataset mask")
        val (out, _) = RST_InitNoData.execute(src, Map.empty)
        try {
            val nd = new Array[java.lang.Double](1)
            out.GetRasterBand(1).GetNoDataValue(nd)
            assert(Option(nd(0)).map(_.doubleValue()).contains(65535.0))
            val vals = new Array[Int](W * H)
            val mask = new Array[Byte](W * H)
            out.GetRasterBand(1).ReadRaster(0, 0, W, H, vals)
            out.GetRasterBand(1).GetMaskBand().ReadRaster(0, 0, W, H, mask)
            val unmasked = values.indices.filter(i => srcMask(i) == 0 && mask(i) != 0)
            assert(unmasked.isEmpty, s"masked source pixels became valid at ${unmasked.take(5)}")
            // A masked source keeps gdalwarp's handling of a fallback-valued pixel, so it is not checked.
            val changed = values.indices.filter(i =>
                i != fallbackAt && srcMask(i) != 0 && (mask(i) == 0 || vals(i) != values(i)))
            assert(changed.isEmpty, s"unmasked pixels changed at ${changed.take(5)}")
        } finally {
            RasterDriver.releaseDataset(out)
            src.delete()
            gdal.Unlink(path)
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
