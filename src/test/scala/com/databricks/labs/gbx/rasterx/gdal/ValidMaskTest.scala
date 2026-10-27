package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.Dataset
import org.gdal.gdalconst.gdalconstConstants._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

/**
  * The one alpha / validity rule of the heavyweight tier ([[GDAL.alphaBandIndex]], [[ValidMask]]), the
  * counterpart of the lightweight tier's `_nodata.alpha_index` and `valid_mask` and tested on the same 4 x 4
  * pattern. The alpha band is the LAST band, when it is labelled alpha. A pixel is valid when its alpha is
  * above 0 and some data band is valid by its mask, at every band count (GDAL derives a mask from alpha only
  * for 2- and 4-band rasters).
  *
  * Rows of the pattern: 0 = every band 7 and the last band 0; 1 = every band 0 and the last band 255;
  * 2 = every band 0; 3 = every band 7 and the last band 255.
  */
class ValidMaskTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit =
        GDALManager.init(ExpressionConfig(Map.empty[String, String], new SerializableConfiguration(new Configuration())))

    private case class Layout(name: String, interps: Seq[Int]) {
        def alphaLast: Boolean = interps.last == GCI_AlphaBand
    }

    private val Layouts = Seq(
      Layout("rgba", Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand, GCI_AlphaBand)),
      Layout("gray_alpha", Seq(GCI_GrayIndex, GCI_AlphaBand)),
      Layout("gray_x_alpha", Seq(GCI_GrayIndex, GCI_Undefined, GCI_AlphaBand)),
      Layout("rgb_x_alpha", Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand, GCI_Undefined, GCI_Undefined, GCI_AlphaBand)),
      Layout("only_alpha", Seq(GCI_AlphaBand)),
      Layout("rgba_x", Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand, GCI_AlphaBand, GCI_Undefined)),
      Layout("rgb_nir", Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand, GCI_Undefined))
    )

    private def fixture(l: Layout, nodata: Option[Double]): Dataset = {
        val rgb = l.interps.take(3) == Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand)
        val ds = GDALManager.gtiffDriver().Create(
          s"/vsimem/validmask_${UUID.randomUUID().toString.replace("-", "")}.tif", 4, 4, l.interps.size, GDT_Byte,
          Array(s"PHOTOMETRIC=${if (rgb) "RGB" else "MINISBLACK"}")
        )
        l.interps.zipWithIndex.foreach { case (ci, i) => ds.GetRasterBand(i + 1).SetColorInterpretation(ci) }
        l.interps.indices.foreach { i =>
            val last = i == l.interps.size - 1
            val arr = Array.tabulate(16) { p =>
                p / 4 match {
                    case 0 => if (last) 0.0 else 7.0
                    case 1 => if (last) 255.0 else 0.0
                    case 2 => 0.0
                    case _ => if (last) 255.0 else 7.0
                }
            }
            val band = ds.GetRasterBand(i + 1)
            nodata.foreach(v => band.SetNoDataValue(v))
            band.WriteRaster(0, 0, 4, 4, arr)
        }
        ds.FlushCache()
        l.interps.indices.foreach(i => assert(ds.GetRasterBand(i + 1).GetColorInterpretation == l.interps(i), s"fixture ${l.name}"))
        ds
    }

    private def validRows(ds: Dataset): Seq[Int] = {
        val buf = new Array[Byte](16)
        new ValidMask(ds).read(0, 0, 4, 4, buf)
        (0 until 4).filter(r => (0 until 4).forall(c => buf(r * 4 + c) != 0))
    }

    for (l <- Layouts) {
        test(s"the alpha band of ${l.name} is its last band, when that is labelled alpha, and a copy is labelled to match") {
            val ds = fixture(l, None)
            try {
                assert(GDAL.alphaBandIndex(ds) == (if (l.alphaLast) Some(l.interps.size) else None))
                assert(GDAL.hasAlpha(ds) == l.alphaLast)
                // a copy is labelled alpha where the source's alpha band is, whatever GDAL labelled it by default
                val copy = ds.GetDriver().Create(
                  s"/vsimem/validmask_copy_${UUID.randomUUID().toString.replace("-", "")}.tif", 4, 4, l.interps.size, GDT_Byte
                )
                try {
                    GDAL.matchAlpha(copy, GDAL.alphaBandIndex(ds))
                    val labelled = (1 to copy.GetRasterCount()).filter(copy.GetRasterBand(_).GetColorInterpretation == GCI_AlphaBand)
                    assert(labelled == GDAL.alphaBandIndex(ds).toSeq)
                } finally RasterDriver.releaseDataset(copy)
            } finally RasterDriver.releaseDataset(ds)
        }

        for (nodata <- Seq(None, Some(0.0))) {
            val rows =
                if (l.alphaLast) (if (nodata.isEmpty) Seq(1, 3) else Seq(3))
                else if (nodata.isEmpty) Seq(0, 1, 2, 3)
                else Seq(0, 1, 3)
            // a raster that is only an alpha band is judged by its alpha values, whatever NoData it declares
            val expected = if (l.name == "only_alpha") Seq(1, 3) else rows
            test(s"the validity of ${l.name} with NoData $nodata: valid rows $expected") {
                val ds = fixture(l, nodata)
                try {
                    assert(validRows(ds) == expected)
                    assert(ValidMask.anyValid(ds) == expected.nonEmpty)
                } finally RasterDriver.releaseDataset(ds)
            }
        }
    }

    test("anyValid is false for a raster that is entirely transparent, and reads in strips") {
        val l = Layouts.head
        val ds = fixture(l, None)
        try {
            ds.GetRasterBand(4).WriteRaster(0, 0, 4, 4, Array.fill(16)(0.0))
            ds.FlushCache()
            assert(!ValidMask.anyValid(ds))
        } finally RasterDriver.releaseDataset(ds)
    }

    // The band-subset rule, the same cases as the lightweight tier's `subset_colorinterp`.
    private val SubsetCases: Seq[(Layout, Seq[Int], Option[Int])] = Seq(
      (Layouts(0), Seq(4), None), // a lone alpha band is data
      (Layouts(5), Seq(2, 4), None), // rgba_x: band 4 is data
      (Layouts(5), Seq(4), None),
      (Layouts(0), Seq(1, 2, 3, 4), Some(4)), // the alpha band, selected last with data: kept
      (Layouts(0), Seq(4, 1), None), // selected first
      (Layouts(0), Seq(1, 4), Some(2)),
      (Layouts(0), Seq(4, 4), None), // no data band
      (Layouts(0), Seq(1), None),
      (Layouts(2), Seq(1, 3), Some(2)),
      (Layouts(2), Seq(3), None),
      (Layouts(6), Seq(1, 4), None) // rgb_nir has no alpha band
    )

    for ((l, bands, expected) <- SubsetCases) {
        test(s"a subset of ${l.name} bands ${bands.mkString(",")} has its alpha band at $expected") {
            val ds = fixture(l, None)
            try {
                assert(GDAL.subsetAlphaIndex(ds, bands) == expected)
                // the translate arguments relabel every band labelled alpha apart from the subset's alpha band
                val args = GDAL.subsetInterpretationArgs(ds, bands)
                val relabelled = args.grouped(2).map(a => a.head.stripPrefix("-colorinterp_").toInt).toSeq
                val labelledAlpha = bands.zipWithIndex.collect {
                    case (b, i) if ds.GetRasterBand(b).GetColorInterpretation == GCI_AlphaBand && !expected.contains(i + 1) => i + 1
                }
                assert(relabelled == labelledAlpha)
                assert(args.grouped(2).forall(_.last == "undefined"))
            } finally RasterDriver.releaseDataset(ds)
        }
    }
}
