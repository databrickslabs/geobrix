package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import org.gdal.gdal.{Dataset, TranslateOptions, gdal}

import java.util.concurrent.atomic.AtomicLong

/**
  * What a gdalwarp of `ds` reads: `ds` itself, or, for a source masked by its own per-dataset mask band
  * ([[GDAL.hasOwnMask]]: an internal or external GTiff mask, as JPEG/YCbCr imagery often has, not an alpha
  * band), its mask view. The view is an in-memory VRT of `ds`: its bands, then that mask as one more band
  * (colour interpretation undefined), which the warp reads as the source alpha ([[alphaArgs]]: `-srcalpha`).
  * gdalwarp ignores a per-dataset mask band whenever a source NoData is set and under a cutline it has to
  * rasterize (a hexagon, for example), but it always applies a source alpha band. As the extra band is not
  * marked alpha, gdalwarp adds no alpha band to the output, which keeps the source's bands and data type.
  * A source with an alpha band, which gdalwarp already applies, and a source without a mask band are warped
  * as they are.
  *
  * Every heavyweight warp of a source that may carry its own mask goes through one: [[warp]] hands gdalwarp
  * [[dataset]] with [[alphaArgs]], and `ds` as the dataset the output's write options and metadata follow.
  * One view serves any number of warps of `ds`. The view reads `ds` through its band handles and owns no
  * file; [[close]] deletes it, and must run before `ds` is released.
  */
final class WarpSource(val ds: Dataset) extends AutoCloseable {

    private var view: Dataset = WarpSource.maskView(ds).orNull

    /** True when the warp reads the mask view. */
    val masked: Boolean = view != null

    /** The dataset gdalwarp reads: the mask view, or `ds`. */
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
        if (!masked && GDAL.hasAlpha(ds)) Seq.empty
        else GDAL.dstNoDataArgs(ds) ++ GDAL.srcNoDataArgs(ds, maskAsAlpha = masked)

    /** [[GDALWarp.executeWarp]] of [[dataset]] with [[alphaArgs]] then `extraArgs`, `ds` standing for the
      * input in the write options and the metadata. The caller owns (and must release) the result. */
    def warp(
        outputPath: String,
        options: Map[String, String],
        command: String,
        extraArgs: Seq[String] = Seq.empty
    ): (Dataset, Map[String, String]) =
        GDALWarp.executeWarp(outputPath, Array(dataset), options, command, alphaArgs ++ extraArgs, like = ds)

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

    /** The mask view of `ds` (see [[WarpSource]]), or None when it has no own mask band or has an alpha band. */
    private def maskView(ds: Dataset): Option[Dataset] = {
        val n = ds.GetRasterCount()
        val maskOf = (1 to n).find(i => GDAL.hasOwnMask(ds.GetRasterBand(i)))
        if (maskOf.isEmpty || GDAL.hasAlpha(ds)) None
        else {
            val args = new java.util.Vector[String]()
            (Seq("-of", "VRT") ++ (1 to n).flatMap(i => Seq("-b", i.toString)) ++
                Seq("-b", s"mask,${maskOf.get}", s"-colorinterp_${n + 1}", "undefined", "-mask", "none"))
                .foreach(args.add)
            val translateOptions = new TranslateOptions(args)
            val view =
                try gdal.Translate("", ds, translateOptions)
                finally translateOptions.delete()
            if (view == null) {
                throw new IllegalStateException(
                  s"WarpSource: could not build the mask view of ${ds.GetDescription}: ${gdal.GetLastErrorMsg}"
                )
            }
            viewsBuilt.incrementAndGet()
            Some(view)
        }
    }
}
