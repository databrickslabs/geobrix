package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver, ValidMask}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.Dataset
import org.gdal.gdalconst.gdalconstConstants._
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

/**
  * The readers that judge each band on its own (the raster-to-grid reducers and the band statistics) count a
  * transparent pixel (alpha 0) for no band, whatever the band count of the raster: GDAL derives a mask from the
  * alpha band for 2- and 4-band rasters only, so a 3-band gray+x+alpha or a 6-band RGB+extra+alpha raster read
  * as all valid. The alpha band itself is summarised by its own values over every pixel, as GDAL's mask for it
  * does (the rule of both tiers, [[ValidMask]]).
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

    /** A `size` x `size` (4 by default) EPSG:4326 GTiff of `l`; `value(row, last)` gives the value of a pixel in `row`
      * (`last`: the last band). With `maskedColumn0` an internal per-dataset mask hides column 0. */
    private def fixture(
        l: Layout,
        value: (Int, Boolean) => Double = AlphaValidityReadersTest.pattern,
        size: Int = 4,
        maskedColumn0: Boolean = false,
        blockSize: Int = 0,
        pixel: Option[(Int, Int) => Double] = None
    ): Dataset = {
        val rgb = l.interps.take(3) == Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand)
        val tiling = if (blockSize > 0) Array("TILED=YES", s"BLOCKXSIZE=$blockSize", s"BLOCKYSIZE=$blockSize") else Array.empty[String]
        val ds = GDALManager.gtiffDriver().Create(
          s"/vsimem/alphareaders_${UUID.randomUUID().toString.replace("-", "")}.tif", size, size, l.count, GDT_Byte,
          Array(s"PHOTOMETRIC=${if (rgb) "RGB" else "MINISBLACK"}") ++ tiling
        )
        ds.SetGeoTransform(Array(10.0, 0.5, 0.0, 50.0, 0.0, -0.5))
        val sr = new SpatialReference()
        sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        sr.delete()
        l.interps.zipWithIndex.foreach { case (c, i) => ds.GetRasterBand(i + 1).SetColorInterpretation(c) }
        (0 until l.count).foreach { i =>
            val last = i == l.count - 1
            val px = pixel.filter(_ => !last || !l.alphaLast)
            ds.GetRasterBand(i + 1).WriteRaster(
              0, 0, size, size, Array.tabulate(size * size)(p => px.map(f => f(p / size, p % size)).getOrElse(value(p / size, last)))
            )
        }
        if (maskedColumn0) {
            ds.CreateMaskBand(GMF_PER_DATASET)
            ds.GetRasterBand(1).GetMaskBand().WriteRaster(
              0, 0, size, size, Array.tabulate[Byte](size * size)(p => if (p % size == 0) 0.toByte else 255.toByte)
            )
        }
        ds.FlushCache()
        (0 until l.count).foreach(i => assert(ds.GetRasterBand(i + 1).GetColorInterpretation == l.interps(i), s"fixture ${l.name}"))
        ds
    }

    /** Per band, the pixels valid for it: a data band of an alpha layout counts the opaque pixels, the alpha band
      * every pixel. */
    private def validCounts(l: Layout): Seq[Int] =
        if (l.alphaLast) Seq.fill(l.count - 1)(8) :+ 16 else Seq.fill(l.count)(16)

    /** The statistics of each band over the pixels valid for it (the shared reader the light tier is held to). */
    private def stats(ds: Dataset): Seq[BandAccessors.ValidStats] =
        (1 to ds.GetRasterCount()).map(i => BandAccessors.validStats(ds, i))

    // ------------------------------------------------------------------------------------------------
    // band statistics and isEmpty
    // ------------------------------------------------------------------------------------------------

    for (l <- Layouts) {
        test(s"the pixel count, minimum, maximum and mean of ${l.name} are over the pixels valid for each band") {
            val ds = fixture(l)
            try {
                val st = stats(ds)
                assert(st.map(_.count) == validCounts(l).map(_.toLong))
                val (mins, maxs) = (st.map(_.min), st.map(_.max))
                if (l.alphaLast) {
                    // band 1: 4 zeros (row 1) and 4 sevens (row 3) over the opaque pixels
                    assert(mins.head == 0.0 && maxs.head == 7.0)
                    assert(st.head.mean == 3.5)
                    // the alpha band: 0 and 255 over every pixel
                    assert(mins.last == 0.0 && maxs.last == 255.0)
                    assert(st.last.mean == 127.5)
                } else assert(mins.head == 0.0 && maxs.head == 7.0)
            } finally RasterDriver.releaseDataset(ds)
        }
    }

    // ------------------------------------------------------------------------------------------------
    // a band masked by its own mask band (no alpha band): GDAL's multi-dimensional statistics ignore it
    // ------------------------------------------------------------------------------------------------

    test("the statistics of a raster with its own mask band count only the unmasked pixels") {
        val ds = fixture(RgbNir, maskedColumn0 = true)
        try {
            assert(GDAL.hasOwnMask(ds.GetRasterBand(1)))
            // columns 1-3 of the pattern: rows 0 and 3 hold 7, rows 1 and 2 hold 0
            val st = stats(ds)
            assert(st.map(_.count) == Seq.fill(4)(12L))
            // band 4 of the fixture holds the pattern's last-band values (0 and 255): 6 of each over the 12 pixels
            assert(st.map(_.mean) == Seq(3.5, 3.5, 3.5, 127.5))
            assert(st.head.min == 0.0 && st.head.max == 7.0)
        } finally RasterDriver.releaseDataset(ds)
    }

    test("GDAL.requireOk passes CE_None and throws, with GDAL's message, for any other code") {
        GDAL.requireOk(CE_None, "a call that worked")
        val e = intercept[IllegalStateException](GDAL.requireOk(CE_Failure, "writing the validity"))
        assert(e.getMessage.startsWith("writing the validity failed"), e.getMessage)
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
