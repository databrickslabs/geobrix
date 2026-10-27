package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.grid.{H3, Quadbin}
import com.databricks.labs.gbx.rasterx.expressions.accessors.{RST_Avg, RST_Max, RST_Median, RST_Min, RST_PixelCount}
import com.databricks.labs.gbx.rasterx.expressions.grid.RasterToGridGeneric
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.Dataset
import org.gdal.gdalconst.gdalconstConstants._
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID
import scala.collection.mutable

/**
  * The readers that judge each band on its own (the raster-to-grid reducers and the band statistics) count a
  * transparent pixel (alpha 0) for no band, whatever the band count of the raster: GDAL derives a mask from the
  * alpha band for 2- and 4-band rasters only, so a 3-band gray+x+alpha or a 6-band RGB+extra+alpha raster read
  * as all valid. The alpha band itself is summarised by its own values over every pixel, as GDAL's mask for it
  * does; `isEmpty` is true for a raster with no valid pixel (the rule of both tiers, [[ValidMask]]).
  *
  * Fixtures are 4 x 4 (rows: 0 = every band 7 and alpha 0; 1 = every band 0 and alpha 255; 2 = every band 0 and
  * alpha 0; 3 = every band 7 and alpha 255), the lightweight tier's `al.pattern`, so band 1 of an alpha layout
  * has 4 zeros and 4 sevens over its 8 opaque pixels.
  */
class AlphaValidityReadersTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit =
        GDALManager.init(ExpressionConfig(Map.empty[String, String], new SerializableConfiguration(new Configuration())))

    private case class Layout(name: String, interps: Seq[Int]) {
        def count: Int         = interps.size
        def alphaLast: Boolean = interps.last == GCI_AlphaBand
    }

    private val Rgba   = Layout("rgba", Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand, GCI_AlphaBand))
    private val GrayA  = Layout("gray_alpha", Seq(GCI_GrayIndex, GCI_AlphaBand))
    private val GrayXA = Layout("gray_x_alpha", Seq(GCI_GrayIndex, GCI_Undefined, GCI_AlphaBand))
    private val RgbXA  = Layout("rgb_x_alpha", Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand, GCI_Undefined, GCI_Undefined, GCI_AlphaBand))
    private val RgbaX  = Layout("rgba_x", Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand, GCI_AlphaBand, GCI_Undefined))
    private val RgbNir = Layout("rgb_nir", Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand, GCI_Undefined))
    private val Layouts = Seq(Rgba, GrayA, GrayXA, RgbXA, RgbaX, RgbNir)

    /** A 4 x 4 EPSG:4326 GTiff of `l`; `pattern(row, last)` gives the value of a pixel in `row`. */
    private def fixture(l: Layout, value: (Int, Boolean) => Double = AlphaValidityReadersTest.pattern): Dataset = {
        val rgb = l.interps.take(3) == Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand)
        val ds = GDALManager.gtiffDriver().Create(
          s"/vsimem/alphareaders_${UUID.randomUUID().toString.replace("-", "")}.tif", 4, 4, l.count, GDT_Byte,
          Array(s"PHOTOMETRIC=${if (rgb) "RGB" else "MINISBLACK"}")
        )
        ds.SetGeoTransform(Array(10.0, 0.5, 0.0, 50.0, 0.0, -0.5))
        val sr = new SpatialReference()
        sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        sr.delete()
        l.interps.zipWithIndex.foreach { case (c, i) => ds.GetRasterBand(i + 1).SetColorInterpretation(c) }
        (0 until l.count).foreach { i =>
            val last = i == l.count - 1
            ds.GetRasterBand(i + 1).WriteRaster(0, 0, 4, 4, Array.tabulate(16)(p => value(p / 4, last)))
        }
        ds.FlushCache()
        (0 until l.count).foreach(i => assert(ds.GetRasterBand(i + 1).GetColorInterpretation == l.interps(i), s"fixture ${l.name}"))
        ds
    }

    /** Per band, the pixels valid for it: a data band of an alpha layout counts the opaque pixels, the alpha band
      * every pixel. */
    private def validCounts(l: Layout): Seq[Int] =
        if (l.alphaLast) Seq.fill(l.count - 1)(8) :+ 16 else Seq.fill(l.count)(16)

    // ------------------------------------------------------------------------------------------------
    // raster-to-grid reducers
    // ------------------------------------------------------------------------------------------------

    private val grids = Seq(("h3", H3, 4), ("quadbin", Quadbin, 6))

    for ((name, grid, res) <- grids; l <- Layouts) {
        test(s"$name centroid raster-to-grid of ${l.name} aggregates the pixels valid for each band") {
            val ds = fixture(l)
            try {
                val bands = RasterToGridGeneric.execute[Seq[Double]](grid, ds, res, (buf: mutable.ArrayBuffer[Double]) => buf.toSeq)
                assert(bands.map(_.map(_._2.size).sum).toSeq == validCounts(l))
            } finally RasterDriver.releaseDataset(ds)
        }

        test(s"$name covering raster-to-grid of ${l.name} aggregates the pixels valid for each band") {
            val ds = fixture(l)
            try {
                val bands = RasterToGridGeneric.execute[Double](
                  grid, ds, res, "sparse", "covering", (b: mutable.ArrayBuffer[Double]) => b.size.toDouble,
                  (b: mutable.ArrayBuffer[(Double, Double)]) => b.map(_._2).sum, None
                )
                val totals = bands.map(_.map(_._2.getOrElse(0.0)).sum).toSeq
                assert(totals.zip(validCounts(l)).forall { case (t, w) => math.abs(t - w) < 1e-6 }, s"$totals != ${validCounts(l)}")
            } finally RasterDriver.releaseDataset(ds)
        }
    }

    // ------------------------------------------------------------------------------------------------
    // band statistics and isEmpty
    // ------------------------------------------------------------------------------------------------

    for (l <- Layouts) {
        test(s"the pixel count, minimum, maximum and mean of ${l.name} are over the pixels valid for each band") {
            val ds = fixture(l)
            try {
                assert(RST_PixelCount.execute(ds).toSeq == validCounts(l).map(_.toLong))
                val (mins, maxs) = (RST_Min.execute(ds).map(_.doubleValue).toSeq, RST_Max.execute(ds).map(_.doubleValue).toSeq)
                if (l.alphaLast) {
                    // band 1: 4 zeros (row 1) and 4 sevens (row 3) over the opaque pixels
                    assert(mins.head == 0.0 && maxs.head == 7.0)
                    assert(RST_Avg.execute(ds).head.doubleValue == 3.5)
                    // the alpha band: 0 and 255 over every pixel
                    assert(mins.last == 0.0 && maxs.last == 255.0)
                    assert(RST_Avg.execute(ds).last.doubleValue == 127.5)
                } else assert(mins.head == 0.0 && maxs.head == 7.0)
            } finally RasterDriver.releaseDataset(ds)
        }
    }

    test("the median of a 3-band gray+x+alpha raster is over its opaque pixels") {
        // 12 transparent pixels hold 99 and the 4 opaque ones hold 5: the median is 99 over every pixel, 5 over the
        // opaque ones
        val ds = fixture(GrayXA, (row, last) => if (last) (if (row == 3) 255.0 else 0.0) else (if (row == 3) 5.0 else 99.0))
        try {
            val median = RST_Median.execute(ds, Map.empty[String, String]).head
            assert(median.doubleValue == 5.0, s"median $median")
        } finally RasterDriver.releaseDataset(ds)
    }

    for (l <- Seq(Rgba, GrayA, GrayXA, RgbXA)) {
        test(s"${l.name} is empty exactly when it is entirely transparent") {
            val opaque      = fixture(l)
            val transparent = fixture(l, (_, last) => if (last) 0.0 else 9.0)
            try {
                assert(!RasterAccessors.isEmpty(opaque))
                assert(RasterAccessors.isEmpty(transparent))
            } finally {
                RasterDriver.releaseDataset(opaque)
                RasterDriver.releaseDataset(transparent)
            }
        }
    }
}

object AlphaValidityReadersTest {

    /** Rows of the pattern: 0 = every band 7 and the last band 0; 1 = every band 0 and the last band 255; 2 = every
      * band 0 and the last band 0; 3 = every band 7 and the last band 255. */
    val pattern: (Int, Boolean) => Double = {
        case (0, last) => if (last) 0.0 else 7.0
        case (1, last) => if (last) 255.0 else 0.0
        case (2, _)    => 0.0
        case (_, last) => if (last) 255.0 else 7.0
    }
}
