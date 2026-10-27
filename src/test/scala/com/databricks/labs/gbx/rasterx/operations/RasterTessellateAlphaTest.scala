package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.Dataset
import org.gdal.gdalconst.gdalconstConstants._
import org.gdal.osr.SpatialReference
import org.gdal.osr.osrConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID
import scala.collection.mutable

/**
  * Alpha bands in heavy tessellation chips. The alpha band is the LAST band, when it is labelled alpha
  * (gdalwarp's rule, [[GDAL.alphaBandIndex]]); a band labelled alpha anywhere else is data. Fixtures are
  * /vsimem GTiffs of several band layouts:
  *
  *  - centroid chips of an UNWARPED alpha source (H3, quadbin, BNG when the source is already in EPSG:27700)
  *    declare no NoData over the alpha band, so GDAL masks them by alpha and transparent pixels are NoData;
  *  - a pixel with alpha 0 is assigned to no cell, so sparse emits no all-transparent cell, whatever the
  *    band count (GDAL masks by alpha only for 2- and 4-band rasters);
  *  - a chip is labelled alpha exactly where its source is: a non-alpha fourth band of a uint8 raster stays
  *    data, and the alpha band of a 16-bit RGBA raster stays alpha;
  *  - a raster that is only an alpha band is judged by its alpha values.
  */
class RasterTessellateAlphaTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit =
        GDALManager.init(ExpressionConfig(Map.empty[String, String], new SerializableConfiguration(new Configuration())))

    private val N = 40
    private val H3Res = 7
    private val BngRes = 3
    private val QuadbinRes = 12
    private val CustomRes = 4

    /** Band layouts: the colour interpretation of each band and whether the last one is alpha. */
    private case class Layout(name: String, interps: Seq[Int], dtype: Int = GDT_Byte) {
        def count: Int = interps.size
        def alphaLast: Boolean = interps.last == GCI_AlphaBand
    }

    private val Rgba   = Layout("rgba", Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand, GCI_AlphaBand))
    private val GrayXA = Layout("gray_x_alpha", Seq(GCI_GrayIndex, GCI_Undefined, GCI_AlphaBand))
    private val RgbXA  = Layout("rgb_x_alpha", Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand, GCI_Undefined, GCI_Undefined, GCI_AlphaBand))
    private val GrayA  = Layout("gray_alpha", Seq(GCI_GrayIndex, GCI_AlphaBand))
    private val RgbaX  = Layout("rgba_x", Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand, GCI_AlphaBand, GCI_Undefined))
    private val RgbNir = Layout("rgb_nir", Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand, GCI_Undefined))
    private val Rgba16 = Layout("rgba16", Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand, GCI_AlphaBand), GDT_UInt16)
    private val OnlyAlpha = Layout("only_alpha", Seq(GCI_AlphaBand))

    /** Where a grid's fixtures sit: CRS, geotransform, resolution. */
    private case class Site(grid: String, epsg: Int, gt: Array[Double], res: Int)

    private val Norfolk = Array(1.00, 0.2 / N, 0.0, 52.60, 0.0, -0.2 / N)
    private val Gb      = Array(620000.0, 500.0, 0.0, 310000.0, 0.0, -500.0)
    private val H3Site       = Site("h3", 4326, Norfolk, H3Res)
    private val QuadbinSite  = Site("quadbin", 4326, Norfolk, QuadbinRes)
    private val BngNativeSite = Site("bng", 27700, Gb, BngRes)
    private val CustomSite   = Site("custom", 27700, Gb, CustomRes)
    private val Sites = Seq(H3Site, QuadbinSite, BngNativeSite, CustomSite)

    /** A custom grid in EPSG:27700 whose cells at `CustomRes` (6.25 km) tile the Great Britain fixture. */
    private val CustomGrid = CustomGridSystem(GridConf(0, 1000000, 0, 1000000, 2, 100000, 100000, Some(27700)))

    private def mem(name: String): String = s"/vsimem/alpha_${UUID.randomUUID().toString.replace("-", "")}_$name"

    /**
      * A /vsimem N x N GTiff of layout `l` at `site`, no NoData unless `nodata`: colour bands hold non-zero
      * values (p * band-multiplier % 199 + 1) and the alpha band (the last band of an alpha layout) is 255,
      * except `transparent` rectangles `(x, y, w, h)`, where it is 0 under the same non-zero colour.
      */
    private def source(l: Layout, site: Site, transparent: Seq[(Int, Int, Int, Int)] = Nil, nodata: Option[Double] = None): Dataset = {
        val rgb = l.interps.take(3) == Seq(GCI_RedBand, GCI_GreenBand, GCI_BlueBand)
        val opts = Array(s"PHOTOMETRIC=${if (rgb) "RGB" else "MINISBLACK"}")
        val ds = GDALManager.gtiffDriver().Create(mem(s"${l.name}.tif"), N, N, l.count, l.dtype, opts)
        ds.SetGeoTransform(site.gt)
        val srs = new SpatialReference()
        srs.ImportFromEPSG(site.epsg)
        srs.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        ds.SetProjection(srs.ExportToWkt())
        srs.delete()
        l.interps.zipWithIndex.foreach { case (ci, i) => ds.GetRasterBand(i + 1).SetColorInterpretation(ci) }
        (1 to l.count).foreach { b =>
            val isAlpha = l.alphaLast && b == l.count
            val arr = Array.tabulate(N * N) { p =>
                if (isAlpha) {
                    val (x, y) = (p % N, p / N)
                    if (transparent.exists { case (x0, y0, w, h) => x >= x0 && x < x0 + w && y >= y0 && y < y0 + h }) 0.0 else 255.0
                } else (p * (b + 1) % 199 + 1).toDouble
            }
            val band = ds.GetRasterBand(b)
            nodata.foreach(v => band.SetNoDataValue(v))
            band.WriteRaster(0, 0, N, N, arr)
        }
        ds.FlushCache()
        (1 to l.count).foreach { b =>
            assert(ds.GetRasterBand(b).GetColorInterpretation == l.interps(b - 1), s"fixture ${l.name}: band $b label")
        }
        ds
    }

    /** A copy for a tessellation to own (it releases its input); the fixture stays readable. */
    private def handOff(src: Dataset): Dataset =
        src.GetDriver().CreateCopy(mem("copy.tif"), src)

    private def tessellate(site: Site, ds: Dataset, assignment: String, coverage: String) = site.grid match {
        case "h3"      => RasterTessellate.tessellateH3Iter(ds, Map.empty[String, String], site.res, assignment, coverage)
        case "quadbin" => RasterTessellate.tessellateQuadbinIter(ds, Map.empty[String, String], site.res, assignment, coverage)
        case "custom"  => RasterTessellate.tessellateCustomIter(CustomGrid, ds, Map.empty[String, String], site.res, assignment, coverage)
        case _         => RasterTessellate.tessellateBngIter(ds, Map.empty[String, String], site.res, assignment, coverage)
    }

    /** Drains the tessellation of a copy of `src`, handing each chip to `f` (released after). */
    private def forEachChip(site: Site, src: Dataset, assignment: String, coverage: String)(f: Dataset => Unit): Int = {
        val it = tessellate(site, handOff(src), assignment, coverage)
        var n = 0
        try {
            while (it.hasNext) {
                val (_, chip, _) = it.next()
                try f(chip) finally RasterDriver.releaseDataset(chip)
                n += 1
            }
        } finally it.asInstanceOf[AutoCloseable].close()
        n
    }

    private def values(ds: Dataset, b: Int): Array[Double] = {
        val vals = new Array[Double](ds.getRasterXSize * ds.getRasterYSize)
        ds.GetRasterBand(b).ReadRaster(0, 0, ds.getRasterXSize, ds.getRasterYSize, vals)
        vals
    }

    private def mask(ds: Dataset, b: Int): Array[Byte] = {
        val m = new Array[Byte](ds.getRasterXSize * ds.getRasterYSize)
        ds.GetRasterBand(b).GetMaskBand().ReadRaster(0, 0, ds.getRasterXSize, ds.getRasterYSize, m)
        m
    }

    private def noDataOf(ds: Dataset, b: Int): Option[Double] = {
        val nd = new Array[java.lang.Double](1)
        ds.GetRasterBand(b).GetNoDataValue(nd)
        Option(nd(0)).map(_.doubleValue())
    }

    private def labels(ds: Dataset): Seq[Int] = (1 to ds.getRasterCount).map(ds.GetRasterBand(_).GetColorInterpretation)

    private def alphaLabelled(ds: Dataset): Seq[Int] = labels(ds).zipWithIndex.collect { case (GCI_AlphaBand, i) => i }

    // ------------------------------------------------------------------------------------------------
    // (a) centroid chips of an unwarped alpha source
    // ------------------------------------------------------------------------------------------------

    for (site <- Sites; coverage <- Seq("sparse", "complete")) {
        test(s"(a) ${site.grid} centroid $coverage on an unwarped RGBA source: no NoData over alpha, transparent pixels are NoData") {
            val src = source(Rgba, site, transparent = Seq((0, 4, N, 5)))
            val declared = mutable.Set.empty[Option[Double]]
            var (chips, alphaMasked, validTransparent, validOpaque) = (0, true, 0L, 0L)
            try {
                chips = forEachChip(site, src, "centroid", coverage) { chip =>
                    (1 to chip.getRasterCount).foreach(b => declared += noDataOf(chip, b))
                    alphaMasked &&= (chip.GetRasterBand(1).GetMaskFlags() & GMF_ALPHA) != 0
                    val (alpha, valid) = (values(chip, 4), mask(chip, 1))
                    var k = 0
                    while (k < alpha.length) {
                        if (valid(k) != 0) { if (alpha(k) == 0.0) validTransparent += 1 else validOpaque += 1 }
                        k += 1
                    }
                }
            } finally RasterDriver.releaseDataset(src)
            assert(chips > 0 && validOpaque > 0, "the chips hold no valid pixel")
            assert(declared == Set(None), s"the chips declare NoData $declared over the source's alpha band")
            assert(alphaMasked, "band 1 of a chip is not masked by its alpha band")
            assert(validTransparent == 0, s"$validTransparent transparent pixels (alpha 0) are valid in the chips")
        }
    }

    // ------------------------------------------------------------------------------------------------
    // (b) + (g) no all-transparent cell, whatever the band count
    // ------------------------------------------------------------------------------------------------

    for (site <- Sites; layout <- Seq(Rgba, GrayA, GrayXA, RgbXA); assignment <- Seq("centroid", "covering")) {
        test(s"(b) ${site.grid} $assignment sparse on ${layout.name}: no all-transparent chip") {
            val src = source(layout, site, transparent = Seq((0, 0, N / 2, N)))
            val last = layout.count
            var empty = 0
            var keptAlpha = true
            val chips =
                try
                    forEachChip(site, src, assignment, "sparse") { chip =>
                        keptAlpha &&= alphaLabelled(chip) == Seq(last - 1)
                        if (!values(chip, last).exists(_ > 0.0)) empty += 1
                    }
                finally RasterDriver.releaseDataset(src)
            assert(chips > 0, "no chips")
            assert(empty == 0, s"$empty of $chips chips are all transparent")
            assert(keptAlpha, "a chip lost the alpha label of its last band")
        }
    }

    // ------------------------------------------------------------------------------------------------
    // chips are labelled alpha exactly where their source is
    // ------------------------------------------------------------------------------------------------

    for (layout <- Seq(RgbNir, Rgba16, RgbaX, Rgba); assignment <- Seq("centroid", "covering"); coverage <- Seq("sparse", "complete")) {
        test(s"$assignment $coverage chips of ${layout.name} are labelled alpha where the source's alpha band is") {
            val src = source(layout, H3Site)
            val want = if (layout.alphaLast) Seq(layout.count - 1) else Seq.empty[Int]
            val seen = mutable.Set.empty[Seq[Int]]
            val chips =
                try forEachChip(H3Site, src, assignment, coverage)(chip => seen += alphaLabelled(chip))
                finally RasterDriver.releaseDataset(src)
            assert(chips > 0, "no chips")
            assert(seen == Set(want), s"chips label bands $seen alpha, expected $want")
        }
    }

    // ------------------------------------------------------------------------------------------------
    // (h) the alpha rule
    // ------------------------------------------------------------------------------------------------

    for (layout <- Seq(Rgba, GrayA, GrayXA, RgbXA, Rgba16, OnlyAlpha, RgbaX, RgbNir)) {
        test(s"(h) GDAL.hasAlpha: ${layout.name} has an alpha band exactly when its last band is labelled alpha") {
            val src = source(layout, H3Site)
            try {
                assert(GDAL.hasAlpha(src) == layout.alphaLast)
            } finally RasterDriver.releaseDataset(src)
        }
    }

    // ------------------------------------------------------------------------------------------------
    // E1: a raster that is only an alpha band
    // ------------------------------------------------------------------------------------------------

    test("(E1) H3 covering+sparse on a raster that is only an alpha band drops the chips that are fully transparent") {
        val src = source(OnlyAlpha, H3Site, transparent = Seq((0, 0, N / 2, N)))
        def perCell(coverage: String): Map[Long, Boolean] = {
            val it = RasterTessellate.tessellateH3Iter(handOff(src), Map.empty[String, String], H3Res, "covering", coverage)
            val out = mutable.Map.empty[Long, Boolean]
            try {
                while (it.hasNext) {
                    val (cell, chip, _) = it.next()
                    try out(cell) = values(chip, 1).exists(_ > 0.0) finally RasterDriver.releaseDataset(chip)
                }
            } finally it.asInstanceOf[AutoCloseable].close()
            out.toMap
        }
        try {
            val (complete, sparse) = (perCell("complete"), perCell("sparse"))
            val blank = complete.collect { case (cell, false) => cell }.toSet
            assert(blank.nonEmpty, "the fixture has no covering cell over the transparent half")
            assert(sparse.keySet == complete.keySet -- blank, s"sparse kept ${sparse.keySet & blank}")
            assert(sparse.values.forall(identity), "a sparse chip is fully transparent")
        } finally RasterDriver.releaseDataset(src)
    }
}
