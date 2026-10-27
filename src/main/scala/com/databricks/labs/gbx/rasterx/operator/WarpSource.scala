package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import org.gdal.gdal.{Dataset, TranslateOptions, gdal}
import org.gdal.gdalconst.gdalconstConstants.GCI_AlphaBand

import java.util.concurrent.atomic.AtomicLong

/**
  * What a gdalwarp of `ds` reads: `ds` itself, or, for a source masked by its own per-dataset mask band
  * ([[GDAL.hasOwnMask]]: an internal or external GTiff mask, as JPEG/YCbCr imagery often has, not the mask
  * GDAL derives from an alpha band), its mask view. The view is an in-memory VRT of `ds`: its bands, then
  * that mask as one more band (colour interpretation undefined), which the warp reads as the source alpha
  * ([[alphaArgs]]: `-srcalpha`). gdalwarp ignores a per-dataset mask band whenever a source NoData is set and
  * under a cutline it has to rasterize (a hexagon, for example), but it always applies a source alpha band.
  * As the extra band is not marked alpha, gdalwarp adds no alpha band to the output, which keeps the source's
  * bands and data type. A source that has an alpha band AND an own mask band gets the view as well: GDAL
  * reports the own mask only (it outranks the mask derived from the alpha band), the view's source alpha is
  * that mask, and the alpha band is warped as a data band (the view labels it undefined, so that gdalwarp
  * does not take it for the destination's alpha band) and labelled alpha again in the output ([[warp]]), so
  * it still marks the fill and the transparent pixels.
  * A source with only an alpha band, which gdalwarp already applies, and a source without a mask band are
  * warped as they are.
  *
  * gdalwarp copies the source's band labels to the output, written with the pixels. A band labelled alpha that
  * is not the alpha band ([[GDAL.alphaBandIndex]]: the last band) is data, so the view (an in-memory VRT as
  * well, built only for such a source) relabels it undefined, and the warp writes it as data. That view is not
  * a mask view: [[masked]] stays false and no [[viewsBuilt]] is counted.
  *
  * Every heavyweight warp of a source that may carry its own mask goes through one: [[warp]] hands gdalwarp
  * [[dataset]] with [[alphaArgs]], and `ds` as the dataset the output's write options and metadata follow.
  * One view serves any number of warps of `ds`. The view reads `ds` through its band handles and owns no
  * file; [[close]] deletes it, and must run before `ds` is released.
  */
final class WarpSource(val ds: Dataset) extends AutoCloseable {

    private val built: Option[(Dataset, Boolean)] = WarpSource.view(ds)
    private var view: Dataset = built.map(_._1).orNull

    /** True when the warp reads the mask view. */
    val masked: Boolean = built.exists(_._2)

    /** The dataset gdalwarp reads: the view (mask view and/or relabelled), or `ds`. */
    def dataset: Dataset = if (view != null) view else ds

    /** `-srcalpha` when the warp reads the mask view, so it applies the mask band as the source alpha. */
    def alphaArgs: Seq[String] = if (masked) Seq("-srcalpha") else Seq.empty

    /**
      * gdalwarp arguments that make the warp fill of `ds` NoData when some band declares none: gdalwarp
      * otherwise fills the pixels outside the source footprint with 0 and declares no NoData, so they would
      * read as valid data. [[GDAL.dstNoDataArgs]] gives each undeclared band the dtype constant
      * ([[GDAL.getNoDataConstant]]; a declared band keeps its own value), paired with [[GDAL.srcNoDataArgs]],
      * so source pixels equal to it are NoData too instead of being moved by +/- 1 and kept valid; the mask
      * view keeps a source's own mask, also when the source declares NoData. Empty for a source without a
      * mask band whose bands all declare NoData (the warp carries it), and for a source with an alpha band:
      * gdalwarp carries the alpha band, which already marks the fill and the transparent pixels, and a
      * declared NoData would shadow it. The lists are ONE argument each, so these go through the unsplit warp
      * extra arguments ([[warp]] adds [[alphaArgs]]).
      */
    def fillNoDataArgs: Seq[String] =
        if (GDAL.hasAlpha(ds)) Seq.empty
        else GDAL.dstNoDataArgs(ds) ++ GDAL.srcNoDataArgs(ds, maskAsAlpha = masked)

    /** [[GDALWarp.executeWarp]] of [[dataset]] with [[alphaArgs]] then `extraArgs`, `ds` standing for the
      * input in the write options and the metadata. The caller owns (and must release) the result. */
    def warp(
        outputPath: String,
        options: Map[String, String],
        command: String,
        extraArgs: Seq[String] = Seq.empty
    ): (Dataset, Map[String, String]) = {
        val (result, metadata) =
            GDALWarp.executeWarp(outputPath, Array(dataset), options, command, alphaArgs ++ extraArgs, like = ds)
        // The mask view labelled the alpha band of `ds` undefined for the warp: it is the output's alpha band.
        if (result != null && masked) {
            GDAL.alphaBandIndex(ds).foreach { a =>
                result.GetRasterBand(a).SetColorInterpretation(GCI_AlphaBand)
                result.FlushCache()
            }
        }
        (result, metadata)
    }

    /** Deletes the view; idempotent. The view owns no file, so delete() releases all of it (as for the VRTs of
      * MergeRasters and MergeBands). RasterDriver.releaseDataset must not be used: a VRT's file list names its
      * SOURCE files, which it would unlink (/vsimem) or whose node-cache read lock it would drop. */
    override def close(): Unit =
        if (view != null) {
            view.delete()
            view = null
        }
}

object WarpSource {

    /** Test seam: the number of mask views built. Its exact-delta test asserts assume suites do not run in parallel. */
    private[rasterx] val viewsBuilt = new AtomicLong()

    /** Warps `ds` to `outputPath` with `command` (no write options), the warp fill NoData
      * ([[WarpSource.fillNoDataArgs]]) and a source's own mask kept. The caller owns the result. */
    def warpFillAsNoData(ds: Dataset, outputPath: String, command: String): (Dataset, Map[String, String]) = {
        val source = new WarpSource(ds)
        try source.warp(outputPath, Map.empty[String, String], command, source.fillNoDataArgs)
        finally source.close()
    }

    /** The view of `ds` a warp reads (see [[WarpSource]]) and whether it is a mask view, or None when `ds` has no
      * own mask band and no band labelled alpha apart from its alpha band. */
    private def view(ds: Dataset): Option[(Dataset, Boolean)] = {
        val n = ds.GetRasterCount()
        val maskOf = (1 to n).find(i => GDAL.hasOwnMask(ds.GetRasterBand(i)))
        val alpha = GDAL.alphaBandIndex(ds)
        // Bands labelled alpha that are data, and the alpha band itself when the view adds the source's mask as
        // the source alpha: the warp reads those as data.
        val dataLabelled = (1 to n).filter(i =>
            ds.GetRasterBand(i).GetColorInterpretation == GCI_AlphaBand && (!alpha.contains(i) || maskOf.isDefined)
        )
        if (maskOf.isEmpty && dataLabelled.isEmpty) None
        else {
            val args = new java.util.Vector[String]()
            val maskArgs =
                maskOf.toSeq.flatMap(k =>
                    (1 to n).flatMap(i => Seq("-b", i.toString)) ++ Seq("-b", s"mask,$k", s"-colorinterp_${n + 1}", "undefined")
                )
            (Seq("-of", "VRT") ++ maskArgs ++ dataLabelled.flatMap(i => Seq(s"-colorinterp_$i", "undefined")) ++ Seq("-mask", "none"))
                .foreach(args.add)
            val translateOptions = new TranslateOptions(args)
            val view =
                try gdal.Translate("", ds, translateOptions)
                finally translateOptions.delete()
            if (view == null) {
                throw new IllegalStateException(
                  s"WarpSource: could not build the view of ${ds.GetDescription}: ${gdal.GetLastErrorMsg}"
                )
            }
            if (maskOf.isDefined) viewsBuilt.incrementAndGet()
            Some((view, maskOf.isDefined))
        }
    }
}
