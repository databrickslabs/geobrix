package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_Band
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver, ValidMask}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

/**
  * A band subset keeps the Alpha label only where the source's alpha band is selected last with a data band;
  * otherwise every selected band is data. The alpha band is the LAST band, when labelled alpha
  * ([[GDAL.alphaBandIndex]]). `gdal_translate -b` copies the label, so a lone alpha band comes out labelled
  * alpha and is then judged by its values ([[ValidMask]]): `gbx_rst_band` of an RGBA raster's band 4 would have
  * its zero pixels read as NoData, where the lightweight tier writes the band as data. Heavyweight band
  * extraction (`gbx_rst_band`, `gbx_rst_separatebands`) writes the band as data too.
  */
class BandSubsetAlphaTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit =
        GDALManager.init(ExpressionConfig(Map.empty[String, String], new SerializableConfiguration(new Configuration())))

    private case class Layout(name: String, interps: Seq[Int])
    private val Rgba   = Layout("rgba", Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand, GCI_AlphaBand))
    private val RgbaX  = Layout("rgba_x", Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand, GCI_AlphaBand, GCI_Undefined))
    private val GrayXA = Layout("gray_x_alpha", Seq(GCI_GrayIndex, GCI_Undefined, GCI_AlphaBand))

    /** 4 x 4 uint8, every band 7 except 0 in rows 0 and 2 of the last band (and of band 4 for `rgba_x`). */
    private def source(l: Layout): Dataset = {
        val rgb = l.interps.take(3) == Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand)
        val ds = GDALManager.gtiffDriver().Create(
          s"/vsimem/bandsubset_${UUID.randomUUID().toString.replace("-", "")}.tif", 4, 4, l.interps.size, GDT_Byte,
          Array(s"PHOTOMETRIC=${if (rgb) "RGB" else "MINISBLACK"}")
        )
        l.interps.zipWithIndex.foreach { case (ci, i) => ds.GetRasterBand(i + 1).SetColorInterpretation(ci) }
        l.interps.indices.foreach { i =>
            val zeroRows = i == l.interps.size - 1 || (l.name == "rgba_x" && i == 3)
            val arr = Array.tabulate(16)(p => if (zeroRows && (p / 4 == 0 || p / 4 == 2)) 0.0 else 7.0)
            ds.GetRasterBand(i + 1).WriteRaster(0, 0, 4, 4, arr)
        }
        ds.FlushCache()
        ds
    }

    private def labels(ds: Dataset): Seq[Int] = (1 to ds.getRasterCount).map(ds.GetRasterBand(_).GetColorInterpretation)

    /** The labels written into the file of `ds`, read from a copy of the file's own bytes (the handle is closed). */
    private def fileLabels(ds: Dataset): Seq[Int] = {
        val path = ds.GetDescription
        ds.FlushCache()
        ds.delete()
        val bytes = gdal.GetMemFileBuffer(path)
        val copy = s"/vsimem/bandsubset_file_${UUID.randomUUID().toString.replace("-", "")}.tif"
        gdal.FileFromMemBuffer(copy, bytes)
        val reopened = gdal.Open(copy)
        try labels(reopened)
        finally {
            reopened.delete()
            gdal.Unlink(copy)
            gdal.Unlink(path)
        }
    }

    private def allValid(ds: Dataset): Boolean = {
        val n = ds.getRasterXSize * ds.getRasterYSize
        val buf = new Array[Byte](n)
        new ValidMask(ds).read(0, 0, ds.getRasterXSize, ds.getRasterYSize, buf)
        buf.forall(_ != 0)
    }

    for (l <- Seq(Rgba, RgbaX, GrayXA)) {
        test(s"gbx_rst_band of the alpha-labelled band of ${l.name} writes it as data, valid wherever its values and NoData say") {
            val src = source(l)
            val band = l.interps.indexOf(GCI_AlphaBand) + 1
            try {
                val (res, _) = RST_Band.execute(src, Map.empty[String, String], band)
                try {
                    assert(!GDAL.hasAlpha(res), s"band $band of ${l.name} is labelled alpha: ${labels(res)}")
                    assert(allValid(res), "the zero pixels of the band read as NoData")
                    val vals = new Array[Double](16)
                    res.GetRasterBand(1).ReadRaster(0, 0, 4, 4, vals)
                    assert(vals.count(_ == 0.0) == 8, "the band's values are not the source's")
                } finally RasterDriver.releaseDataset(res)
            } finally RasterDriver.releaseDataset(src)
        }

        test(s"gbx_rst_separatebands of ${l.name} writes no band as alpha") {
            val src = source(l)
            try {
                (0 until l.interps.size).foreach { b =>
                    val (res, _) = SeparateBands.getTile(src, Map.empty[String, String], b)
                    try {
                        assert(!GDAL.hasAlpha(res), s"band ${b + 1} of ${l.name} is labelled alpha: ${labels(res)}")
                        assert(allValid(res), s"band ${b + 1} reads partly as NoData")
                    } finally RasterDriver.releaseDataset(res)
                }
            } finally RasterDriver.releaseDataset(src)
        }
    }

    test("gbx_rst_band keeps the other labels: band 1 of an RGBA raster stays red") {
        val src = source(Rgba)
        try {
            val (res, _) = RST_Band.execute(src, Map.empty[String, String], 1)
            try assert(labels(res) == Seq(GCI_RedBand))
            finally RasterDriver.releaseDataset(res)
        } finally RasterDriver.releaseDataset(src)
    }

    test("the file of a band extracted from RGBA holds the data label, not just the handle") {
        val src = source(Rgba)
        try {
            val (res, _) = RST_Band.execute(src, Map.empty[String, String], 4)
            assert(!fileLabels(res).contains(GCI_AlphaBand))
        } finally RasterDriver.releaseDataset(src)
    }
}
