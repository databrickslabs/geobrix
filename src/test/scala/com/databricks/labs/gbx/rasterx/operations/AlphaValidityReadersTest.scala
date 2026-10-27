package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.grid.{H3, Quadbin}
import com.databricks.labs.gbx.rasterx.expressions.RST_DerivedBand
import com.databricks.labs.gbx.rasterx.expressions.accessors.{RST_Avg, RST_Max, RST_Median, RST_Min, RST_PixelCount, RST_Summary}
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_Histogram
import com.databricks.labs.gbx.rasterx.expressions.vector.{RST_Isoband, RST_Polygonize}
import com.databricks.labs.gbx.rasterx.expressions.grid.RasterToGridGeneric
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver, ValidMask}
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

    test("the percentile stretch of gray+x+alpha counts only the opaque pixels") {
        // data bands: 10 + row over the opaque rows (1 and 3), 250 over the transparent ones (0 and 2); the
        // percentiles 0 and 100 are the range of the opaque pixels, 11 to 13: the top maps to 255, the bottom to 0.
        // Counted, the 250s of the transparent rows would make the range 10 to 250.
        val ds = fixture(GrayXA, (row, last) => if (last) (if (row == 1 || row == 3) 255.0 else 0.0) else (if (row == 1 || row == 3) 10.0 + row else 250.0))
        try {
            val (out, _) = PercentileStretch.execute(ds, Map.empty[String, String], 0.0, 100.0)
            try {
                val px = new Array[Double](16)
                out.GetRasterBand(1).ReadRaster(0, 0, 4, 4, px)
                val opaque = (0 until 16).filter(p => p / 4 == 1 || p / 4 == 3).map(px(_))
                assert(opaque.max == 255.0 && opaque.min == 0.0, s"the opaque pixels stretch to ${opaque.min}..${opaque.max}")
            } finally RasterDriver.releaseDataset(out)
        } finally RasterDriver.releaseDataset(ds)
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

    // ------------------------------------------------------------------------------------------------
    // a band masked by its own mask band (no alpha band): GDAL's multi-dimensional statistics ignore it
    // ------------------------------------------------------------------------------------------------

    test("the statistics of a raster with its own mask band count only the unmasked pixels") {
        val ds = fixture(RgbNir, maskedColumn0 = true)
        try {
            assert(GDAL.hasOwnMask(ds.GetRasterBand(1)))
            // columns 1-3 of the pattern: rows 0 and 3 hold 7, rows 1 and 2 hold 0
            assert(RST_PixelCount.execute(ds).toSeq == Seq.fill(4)(12L))
            // band 4 of the fixture holds the pattern's last-band values (0 and 255): 6 of each over the 12 pixels
            assert(RST_Avg.execute(ds).map(_.doubleValue).toSeq == Seq(3.5, 3.5, 3.5, 127.5))
            assert(RST_Min.execute(ds).head.doubleValue == 0.0 && RST_Max.execute(ds).head.doubleValue == 7.0)
            // GetHistogram honours the mask band: 6 zeros and 6 sevens
            val h = RST_Histogram.execute(ds, 8, Some(0.0), Some(7.5), includeNodata = false)("band_1")
            assert(h.sum == 12L && h.head == 6L)
            assert(!RasterAccessors.isEmpty(ds))
        } finally RasterDriver.releaseDataset(ds)
    }

    // ------------------------------------------------------------------------------------------------
    // histogram: over the pixels valid for the band, GDAL's own bucket rule
    // ------------------------------------------------------------------------------------------------

    for (l <- Seq(GrayXA, Rgba, RgbXA)) {
        test(s"the histogram of ${l.name} is over the opaque pixels, with GDAL's buckets") {
            // band 1 over the 8 opaque pixels: 4 zeros and 4 sevens. GDAL histograms the reference itself.
            val ds    = fixture(l)
            // the plain raster is 4 x 4: top half 0, bottom half 7, 8 of each: twice the counts of the opaque pixels
            val plain = fixture(GrayXA.copy(interps = Seq(GCI_GrayIndex)), (row, _) => if (row < 2) 0.0 else 7.0, size = 4)
            try {
                val got  = RST_Histogram.execute(ds, 8, None, None, includeNodata = false)("band_1")
                val want = RST_Histogram.execute(plain, 8, None, None, includeNodata = false)("band_1")
                // GDAL's buckets are half-open, so the sevens (the top of the range) are out of both histograms
                assert(got.sum > 0L)
                assert(got.toSeq == want.map(_ / 2).toSeq, s"${got.toSeq} vs ${want.toSeq}")
            } finally {
                RasterDriver.releaseDataset(ds)
                RasterDriver.releaseDataset(plain)
            }
        }
    }

    test("the histogram of a raster larger than a strip adds up the strips, the shorter last one included") {
        // 1100 x 1100 is 1,210,000 pixels: a strip of 953 rows (ValidMask.StripPixels / 1100) and one of 147. Band 1 holds
        // row % 8, and every 10th row is transparent; the buckets are those of [0, 8) over 8 buckets.
        val n  = 1100
        val ds = fixture(GrayXA, (row, last) => if (last) (if (row % 10 == 0) 0.0 else 255.0) else (row % 8).toDouble, size = n)
        try {
            val got = RST_Histogram.execute(ds, 8, Some(0.0), Some(8.0), includeNodata = false)("band_1")
            val want = Array.tabulate(8)(k => (0 until n).count(r => r % 10 != 0 && r % 8 == k).toLong * n)
            assert(got.toSeq == want.toSeq, s"${got.toSeq} vs ${want.toSeq}")
            assert(n * n > ValidMask.StripPixels)
        } finally RasterDriver.releaseDataset(ds)
    }

    test("GDAL.requireOk passes CE_None and throws, with GDAL's message, for any other code") {
        GDAL.requireOk(CE_None, "a call that worked")
        val e = intercept[IllegalStateException](GDAL.requireOk(CE_Failure, "writing the validity"))
        assert(e.getMessage.startsWith("writing the validity failed"), e.getMessage)
    }

    test("the heavy summary holds no statistics to judge: it is the gdalinfo structure of the raster") {
        val ds = fixture(GrayXA)
        try {
            val json = RST_Summary.execute(ds)
            assert(json.contains("\"bands\"") && !json.contains("\"mean\"") && !json.contains("STATISTICS_MEAN"), json)
        } finally RasterDriver.releaseDataset(ds)
    }

    // ------------------------------------------------------------------------------------------------
    // derivedband: invalid where the input is
    // ------------------------------------------------------------------------------------------------

    private val SumFunc =
        "def total(in_ar, out_ar, xoff, yoff, xsize, ysize, rxsize, rysize, radius, gt, **kw):\n" +
            "    out_ar[:] = sum(a.astype('float64') for a in in_ar)\n"

    for ((what, build) <- Seq[(String, () => Dataset)](
      ("gray_x_alpha", () => fixture(GrayXA)),
      ("rgba", () => fixture(Rgba)),
      ("rgb_nir with its own mask", () => fixture(RgbNir, maskedColumn0 = true))
    )) {
        test(s"the band derived from $what is invalid where its input is") {
            val ds = build()
            try {
                val (out, _) = RST_DerivedBand.execute(Seq(ds), Map.empty[String, String], SumFunc, "total")
                try {
                    assert(out.GetRasterCount == 1)
                    val valid = new Array[Byte](16)
                    new ValidMask(out).read(0, 0, 4, 4, valid)
                    val want = new Array[Byte](16)
                    new ValidMask(ds).read(0, 0, 4, 4, want)
                    assert(valid.map(_ != 0).toSeq == want.map(_ != 0).toSeq)
                    assert(valid.exists(_ == 0) && valid.exists(_ != 0))
                } finally RasterDriver.releaseDataset(out)
            } finally RasterDriver.releaseDataset(ds)
        }
    }


    test("Convolve writes every pixel of its output where the pixel is: corners 20, edges 30, interior 45 for a 3x3 sum of 5s") {
        // it used to write each block's interior at the block's origin, shifted, and leave the rest as the input
        val ds = fixture(RgbNir.copy(interps = Seq(GCI_GrayIndex, GCI_Undefined, GCI_Undefined)), (_, _) => 5.0, size = 6)
        try {
            val (out, _) = Convolve.convolve(ds, Map.empty[String, String], Array.fill(3, 3)(1.0))
            try {
                val px = new Array[Double](36)
                out.GetRasterBand(1).ReadRaster(0, 0, 6, 6, px)
                val want = Array.tabulate(36) { p =>
                    val (r, c) = (p / 6, p % 6)
                    val rows = if (r == 0 || r == 5) 2 else 3
                    val cols = if (c == 0 || c == 5) 2 else 3
                    5.0 * rows * cols
                }
                assert(px.toSeq == want.toSeq, px.toSeq.toString)
            } finally RasterDriver.releaseDataset(out)
        } finally RasterDriver.releaseDataset(ds)
    }

    test("Convolve over several blocks gives the zero-padded sum at every pixel, those at the block borders included") {
        // 40 x 40 in tiles of 16 (blocks of 16, 16 and 8): each pixel is the sum of its in-bounds 3 x 3 neighbours, the
        // neighbour blocks' pixels among them, and lands where it is. The values are 0..6, so the sums fit a Byte.
        val (n, value) = (40, (r: Int, c: Int) => ((r * 3 + c * 5 + (r * c) % 4) % 7).toDouble)
        val ds = fixture(RgbNir.copy(interps = Seq(GCI_GrayIndex)), (_, _) => 0.0, size = n, blockSize = 16, pixel = Some(value))
        try {
            assert(ds.GetRasterBand(1).GetBlockXSize == 16 && ds.GetRasterBand(1).GetBlockYSize == 16)
            val (out, _) = Convolve.convolve(ds, Map.empty[String, String], Array.fill(3, 3)(1.0))
            try {
                val px = new Array[Double](n * n)
                out.GetRasterBand(1).ReadRaster(0, 0, n, n, px)
                val want = Array.tabulate(n * n) { p =>
                    val (r, c) = (p / n, p % n)
                    (for (dr <- -1 to 1; dc <- -1 to 1 if r + dr >= 0 && r + dr < n && c + dc >= 0 && c + dc < n) yield value(r + dr, c + dc)).sum
                }
                val bad = (0 until n * n).filter(p => px(p) != want(p))
                if (bad.nonEmpty) fail(s"${bad.size} pixels differ, the first at row ${bad.head / n}, column ${bad.head % n}")
            } finally RasterDriver.releaseDataset(out)
        } finally RasterDriver.releaseDataset(ds)
    }

    test("a filter of a raster with no declared NoData is its value, not NaN") {
        val ds = fixture(RgbNir.copy(interps = Seq(GCI_GrayIndex, GCI_Undefined, GCI_Undefined)), (row, _) => 10.0 + row, size = 6)
        try {
            val (out, _) = KernelFilter.filter(ds, 3, "avg")
            try {
                val px = new Array[Double](36)
                out.GetRasterBand(1).ReadRaster(0, 0, 6, 6, px)
                // rows 2-3 average rows 1-4 over the 3x3 window: (11 + 12 + 13) / 3 = 12 and (12 + 13 + 14) / 3 = 13
                assert(px(2 * 6 + 2) == 12.0 && px(3 * 6 + 2) == 13.0, px.toSeq.toString)
            } finally RasterDriver.releaseDataset(out)
        } finally RasterDriver.releaseDataset(ds)
    }

    // ------------------------------------------------------------------------------------------------
    // focal filters, polygonize, isoband: a transparent pixel is skipped like NoData, at any band count
    // ------------------------------------------------------------------------------------------------

    /** 6 x 6: 5 over rows 0-1 and 4-5, 99 over rows 2-3, which are transparent (alpha 0; the others 255). */
    private def striped(l: Layout): Dataset =
        fixture(l, (row, last) => if (last && l.alphaLast) (if (row == 2 || row == 3) 0.0 else 255.0) else (if (row == 2 || row == 3) 99.0 else 5.0), size = 6)

    for (l <- Seq(GrayXA, Rgba)) {
        test(s"a max filter of ${l.name} skips the transparent neighbours") {
            val ds = striped(l)
            try {
                val (out, _) = KernelFilter.filter(ds, 3, "max")
                try {
                    val px = new Array[Double](36)
                    out.GetRasterBand(1).ReadRaster(0, 0, 6, 6, px)
                    assert(px.forall(_ == 5.0), px.toSeq.toString)
                } finally RasterDriver.releaseDataset(out)
            } finally RasterDriver.releaseDataset(ds)
        }

        test(s"a convolution of ${l.name} equals that of the same raster with its transparent pixels set to 0") {
            // a transparent neighbour adds nothing, as a 0 does; the same Convolve geometry on both
            val ds    = striped(l)
            val plain = fixture(RgbNir.copy(interps = Seq(GCI_GrayIndex, GCI_Undefined, GCI_Undefined)), (row, _) => if (row == 2 || row == 3) 0.0 else 5.0, size = 6)
            try {
                def convolved(d: Dataset): Array[Double] = {
                    val (out, _) = Convolve.convolve(d, Map.empty[String, String], Array.fill(3, 3)(1.0))
                    try { val px = new Array[Double](36); out.GetRasterBand(1).ReadRaster(0, 0, 6, 6, px); px }
                    finally RasterDriver.releaseDataset(out)
                }
                val (a, b) = (convolved(ds), convolved(plain))
                assert(a.toSeq == b.toSeq, s"${a.toSeq} vs ${b.toSeq}")
            } finally {
                RasterDriver.releaseDataset(ds)
                RasterDriver.releaseDataset(plain)
            }
        }

        test(s"polygonize and isoband of ${l.name} leave out the transparent pixels") {
            val ds = striped(l)
            try {
                val polys = RST_Polygonize.execute(ds, 1, 4)
                val values = (0 until polys.numElements()).map(i => polys.getStruct(i, 2).getDouble(1)).toSet
                assert(values == Set(5.0), values.toString)
                val bands = RST_Isoband.execute(ds, Array(0.0, 10.0, 200.0), 1)
                val idx = (0 until bands.numElements()).map(i => bands.getStruct(i, 4).getInt(1)).toSet
                assert(idx == Set(0), idx.toString)
            } finally RasterDriver.releaseDataset(ds)
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
