package com.databricks.labs.gbx.rasterx.gdal

import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants._
import org.gdal.osr.SpatialReference

/** GDAL constants and helpers: WGS84/3857 refs, driver extension, NoData by type, pixel-to/from-geo coordinate conversion. */
object GDAL {

    /** WGS84 spatial reference (EPSG:4326, traditional axis order). Note: val name is WSG84 (historical typo). */
    val WSG84: SpatialReference = {
        val wsg84 = new SpatialReference()
        wsg84.ImportFromEPSG(4326)
        wsg84.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        wsg84
    }

    /** Web Mercator spatial reference (EPSG:3857). */
    val EPSG3857: SpatialReference = {
        val epsg3857 = new SpatialReference()
        epsg3857.ImportFromEPSG(3857)
        epsg3857.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        epsg3857
    }

    /** File extension for the driver (e.g. tif for GTiff); uses FormatLookup when DMD_EXTENSION is null. Driver is deleted before return. */
    def getExtension(driverShortName: String): String = {
        val driver = gdal.GetDriverByName(driverShortName)
        val result = driver.GetMetadataItem("DMD_EXTENSION")
        val toReturn = if (result == null) FormatLookup.formats(driverShortName) else result
        driver.delete()
        toReturn
    }

    /** Returns a NoData sentinel value for the given GDAL data type constant. */
    def getNoDataConstant(gdalType: Int): Double = {
        gdalType match {
            case GDT_Unknown => 0.0
            case GDT_Byte    => 0.0
            // Unsigned Int16 is Char in scala
            // https://www.tutorialspoint.com/scala/scala_data_types.htm
            case GDT_UInt16  => Char.MaxValue.toDouble
            case GDT_Int16   => Short.MinValue.toDouble
            case GDT_UInt32  => 2 * Int.MaxValue.toDouble
            case GDT_Int32   => Int.MinValue.toDouble
            case GDT_Float32 => Float.MinValue.toDouble
            case GDT_Float64 => Double.MinValue
            case _           => 0.0
        }
    }

    /** The NoData state of `ds` for a warp: the bands that declare none, and the value list gdalwarp takes
      * (`-srcnodata` / `-dstnodata`): one value per band, a declared band's own value or `fill(band)`, written as
      * ONE value when they all agree. The single scan of the bands' declared NoData that the warp helpers share. */
    private final case class WarpNoData(undeclared: Seq[org.gdal.gdal.Band], list: String)

    private def warpNoData(ds: org.gdal.gdal.Dataset, fill: org.gdal.gdal.Band => Double): WarpNoData = {
        val perBand = (1 to ds.GetRasterCount()).map { i =>
            val band = ds.GetRasterBand(i)
            val nd = new Array[java.lang.Double](1)
            band.GetNoDataValue(nd)
            (Option(nd(0)).map(_.doubleValue()), band)
        }
        val values = perBand.map { case (declared, band) => declared.getOrElse(fill(band)) }
        val list = values.distinct match {
            case Seq(single) => single.toString
            case _           => values.mkString(" ")
        }
        WarpNoData(perBand.collect { case (None, band) => band }, list)
    }

    /** True when `band` is masked by its own per-dataset mask band that is not an alpha band (an internal or
      * external GTiff mask, for example). gdalwarp ignores such a mask once a source NoData is set, and under
      * a cutline it has to rasterize; it always applies an alpha band. */
    def hasOwnMask(band: org.gdal.gdal.Band): Boolean = {
        val flags = band.GetMaskFlags()
        (flags & GMF_PER_DATASET) != 0 && (flags & GMF_ALPHA) == 0
    }

    /**
      * THE alpha rule of both tiers (the lightweight tier's `_nodata.alpha_index`): the 1-based index of the
      * alpha band of `ds`, which is its LAST band when that band is labelled alpha, as gdalwarp reads a
      * source. A band labelled alpha anywhere else is ordinary data.
      */
    def alphaBandIndex(ds: org.gdal.gdal.Dataset): Option[Int] = {
        val n = ds.GetRasterCount()
        if (n > 0 && ds.GetRasterBand(n).GetColorInterpretation == GCI_AlphaBand) Some(n) else None
    }

    /** True when `ds` has an alpha band ([[alphaBandIndex]]): gdalwarp carries it and applies it as the source
      * mask. */
    def hasAlpha(ds: org.gdal.gdal.Dataset): Boolean = alphaBandIndex(ds).isDefined

    /**
      * THE band-subset rule of both tiers (the lightweight tier's `_nodata.subset_colorinterp`): the 1-based
      * position of the alpha band in the subset `bands` (1-based source indices, in output order) of `ds`, if
      * any. A subset keeps the Alpha label only when ALL of these hold: the selected band is the source's alpha
      * band ([[alphaBandIndex]]), it is selected LAST, and at least one data band is selected with it. Otherwise
      * every selected band is data, so a lone alpha band, selected for its values, is valid wherever its own
      * NoData or mask says so, and an alpha label on any other band stays data.
      */
    def subsetAlphaIndex(ds: org.gdal.gdal.Dataset, bands: Seq[Int]): Option[Int] =
        alphaBandIndex(ds).filter(a => bands.nonEmpty && bands.last == a && bands.init.exists(_ != a)).map(_ => bands.size)

    /** `gdal_translate` arguments (`-colorinterp_<position> undefined`) that write every band of the subset
      * `bands` of `ds` labelled alpha in the source as data, except the alpha band of the subset
      * ([[subsetAlphaIndex]]): `gdal_translate -b` copies the labels. Empty when nothing needs relabelling. */
    def subsetInterpretationArgs(ds: org.gdal.gdal.Dataset, bands: Seq[Int]): Seq[String] = {
        val alpha = subsetAlphaIndex(ds, bands)
        bands.zipWithIndex.collect {
            case (b, i) if ds.GetRasterBand(b).GetColorInterpretation == GCI_AlphaBand && !alpha.contains(i + 1) =>
                Seq(s"-colorinterp_${i + 1}", "undefined")
        }.flatten
    }

    /**
      * Labels the bands of `dst`, a new dataset holding (windows of) the bands of a source, alpha exactly where
      * the source's alpha band is (`alpha`, from [[alphaBandIndex]] of the source): GDAL labels band 4 of a 4-band
      * uint8 GTiff alpha when it creates one, whatever the source was, and gives a 16-bit RGBA or a gray+alpha
      * GTiff no alpha band. A non-alpha 4th band (near-infrared) then reads as a transparency mask. Nothing else
      * is relabelled, and the call is made before the pixels are written (GDAL ignores a label set after).
      * The lightweight tier's `compression.match_alpha`.
      */
    def matchAlpha(dst: org.gdal.gdal.Dataset, alpha: Option[Int]): Unit =
        (1 to dst.GetRasterCount()).foreach { i =>
            val band = dst.GetRasterBand(i)
            val current = band.GetColorInterpretation
            if (alpha.contains(i)) { if (current != GCI_AlphaBand) band.SetColorInterpretation(GCI_AlphaBand) }
            else if (current == GCI_AlphaBand) band.SetColorInterpretation(GCI_Undefined)
        }

    /** The colour interpretation (`GCI_*`) of every band of `ds`, in order. */
    def colorInterpretations(ds: org.gdal.gdal.Dataset): Seq[Int] =
        (1 to ds.GetRasterCount()).map(ds.GetRasterBand(_).GetColorInterpretation)

    /**
      * [[matchAlpha]] that also copies the other labels: labels the bands of `dst`, a new dataset holding
      * (windows of) the bands of a source whose bands are labelled `labels` ([[colorInterpretations]]), band
      * by band. The alpha label goes only on the source's alpha band ([[alphaBandIndex]] rule: the LAST band,
      * when labelled alpha); an alpha label anywhere else is data (undefined); gray, undefined, red, green and
      * blue are copied, so RGB and the data bands of a gray+x+alpha or a 16-bit RGBA raster are not left to
      * the defaults GDAL gives a new GTiff (red and green for the first two bands of a 3-band uint8 raster).
      * Any other label (palette, CMYK, YCbCr ...) goes with a colour table or photometric `dst` may not
      * have, and is left to GDAL. A band count that differs from `labels` is left as GDAL labelled it. Made
      * before the pixels are written (GDAL ignores a label set after). The lightweight tier's
      * `compression.match_alpha(all_labels=True)`.
      */
    def matchLabels(dst: org.gdal.gdal.Dataset, labels: Seq[Int]): Unit =
        if (labels.size == dst.GetRasterCount()) {
            val alpha = if (labels.last == GCI_AlphaBand) Some(labels.size) else None
            val plain = Set(GCI_GrayIndex, GCI_Undefined, GCI_RedBand, GCI_GreenBand, GCI_BlueBand)
            labels.zipWithIndex.foreach { case (label, i) =>
                val band    = dst.GetRasterBand(i + 1)
                val current = band.GetColorInterpretation
                val want =
                    if (alpha.contains(i + 1)) GCI_AlphaBand
                    else if (plain.contains(label)) label
                    else if (label == GCI_AlphaBand || current == GCI_AlphaBand) GCI_Undefined
                    else current
                if (want != current) band.SetColorInterpretation(want)
            }
        }

    /** True when `ds` has an alpha band ([[alphaBandIndex]]) beside at least one data band, so the statistics of
      * its data bands have to be judged by [[com.databricks.labs.gbx.rasterx.gdal.ValidMask.readBand]]: GDAL
      * derives a mask from the alpha band for 2- and 4-band rasters only, drops it when a NoData or a mask band
      * of the dataset is declared, and the multi-dimensional statistics (`AsMDArray().GetStatistics()`) apply
      * no mask derived from an alpha band at all, so they count the transparent pixels of an RGBA raster too. */
    def hasAlphaBesideData(ds: org.gdal.gdal.Dataset): Boolean =
        ds.GetRasterCount() > 1 && alphaBandIndex(ds).isDefined

    /** True when the multi-dimensional statistics GDAL computes for the bands of `ds`
      * (`AsMDArray().GetStatistics()`: `rst_avg`, `rst_pixelcount`) would count pixels that are not valid for the
      * band, so they are computed over [[com.databricks.labs.gbx.rasterx.gdal.ValidMask.readBand]] instead: `ds`
      * has an alpha band beside data bands ([[hasAlphaBesideData]]), or a band masked by its own mask band
      * ([[hasOwnMask]]), which those statistics ignore. (`ComputeRasterMinMax` and `GetHistogram` honour an own
      * mask band.) */
    def validityJudgesStatistics(ds: org.gdal.gdal.Dataset): Boolean =
        hasAlphaBesideData(ds) || (1 to ds.GetRasterCount()).exists(i => hasOwnMask(ds.GetRasterBand(i)))

    /**
      * gdalwarp arguments that make the source pixels equal to a band's fill value NoData, for the bands of
      * `ds` that declare no NoData: `-srcnodata <one value per band>` (a declared band keeps its own value)
      * and `-wo UNIFIED_SRC_NODATA=NO`, so each band is judged on its own (GDAL otherwise treats a pixel
      * as NoData only when every band matches). Without them, gdalwarp `-dstnodata X` moves a valid source
      * pixel equal to X to X +/- 1 and keeps it valid. `fill` gives an undeclared band's value, by default
      * [[getNoDataConstant]] for its type. Empty when every band declares NoData, or when a band without
      * NoData has its own mask ([[hasOwnMask]]): a source NoData makes gdalwarp drop that mask, so masked
      * pixels would turn valid (an alpha band still masks). `maskAsAlpha` says the warp reads that mask as
      * a source alpha band instead (the mask view of [[com.databricks.labs.gbx.rasterx.operator.WarpSource]]),
      * which a source NoData does not drop, so the arguments are given then. The list is ONE argument: pass
      * it as a warp extra argument, not through a space-split command string.
      */
    def srcNoDataArgs(
        ds: org.gdal.gdal.Dataset,
        fill: org.gdal.gdal.Band => Double = b => getNoDataConstant(b.getDataType),
        maskAsAlpha: Boolean = false
    ): Seq[String] = {
        val nd = warpNoData(ds, fill)
        if (nd.undeclared.isEmpty || (!maskAsAlpha && nd.undeclared.exists(hasOwnMask))) Seq.empty
        else Seq("-srcnodata", nd.list, "-wo", "UNIFIED_SRC_NODATA=NO")
    }

    /**
      * gdalwarp `-dstnodata <one value per band>` for `ds`, when some band declares no NoData: an undeclared
      * band gets `fill(band)` (default [[getNoDataConstant]]), a declared band keeps its own value. Empty when
      * every band declares NoData: the warp carries it. Pair it with [[srcNoDataArgs]]; `-dstnodata` alone
      * moves source pixels equal to the value by +/- 1 and keeps them valid. ONE argument, like there.
      */
    def dstNoDataArgs(
        ds: org.gdal.gdal.Dataset,
        fill: org.gdal.gdal.Band => Double = b => getNoDataConstant(b.getDataType)
    ): Seq[String] = {
        val nd = warpNoData(ds, fill)
        if (nd.undeclared.isEmpty) Seq.empty else Seq("-dstnodata", nd.list)
    }

    /** Converts pixel (x, y) to geographic (xGeo, yGeo) using the 6-element geotransform. */
    def toWorldCoord(gt: Array[Double], x: Int, y: Int): (Double, Double) = {
        val offset = 0.5 // offset to center of the pixel
        val xGeo = gt(0) + (x + offset) * gt(1) + (y + offset) * gt(2)
        val yGeo = gt(3) + (x + offset) * gt(4) + (y + offset) * gt(5)
        (xGeo, yGeo)
    }

    /** Converts geographic (xGeo, yGeo) to pixel (x, y) using the 6-element geotransform. */
    def fromWorldCoord(gt: Array[Double], xGeo: Double, yGeo: Double): (Int, Int) = {
        val det = gt(1) * gt(5) - gt(2) * gt(4)
        val dx = xGeo - gt(0)
        val dy = yGeo - gt(3)
        val x = ( dx * gt(5) - dy * gt(2)) / det
        val y = (-dx * gt(4) + dy * gt(1)) / det
        (x.toInt, y.toInt)
    }

}
