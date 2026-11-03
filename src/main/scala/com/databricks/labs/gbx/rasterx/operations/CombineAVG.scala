package com.databricks.labs.gbx.rasterx.operations

import org.gdal.gdal.Dataset

/**
 * Pixel-wise mean across N input rasters via a VRT Python pixel function.
 *
 * Each input contributes one band; the embedded Python function reads each
 * source's declared NoData (via `BandAccessors.getNoDataValue`, baked into
 * the pyfunc source as a literal list at VRT-write time) and excludes those
 * cells from BOTH the sum and the divisor. Cells with valid value `0` count
 * toward the mean (the previous `>0` mask wrongly excluded them).
 *
 * Output band keeps the VRT's dtype (typically the input dtype). When that
 * dtype is integer, the mean is rounded to nearest int before the unsafe
 * cast — bare truncation produced systematic underbias on Byte / UInt16 EO
 * stacks. When all inputs at a pixel are NoData, the output cell carries the
 * first declared input NoData (or 0 if no input declared one), and that
 * NoData value is also stamped on the output band so downstream consumers
 * can detect all-NoData pixels with `GetNoDataValue`.
 */
object CombineAVG {

    /** Average per-pixel, excluding per-source NoData; preserves first-source NoData on output band. */
    def compute(rasters: Array[Dataset], options: Map[String, String]): (Dataset, Map[String, String]) = {
        RasterAlignment.requireAligned(rasters.toSeq, "gbx_rst_combineavg")
        val sourceNoData: Array[Option[Double]] = rasters.map { ds =>
            val v = BandAccessors.getNoDataValue(ds.GetRasterBand(1))
            if (v.isNaN) None else Some(v)
        }
        val nodataListLiteral = sourceNoData
            .map(_.map(d => f"$d%s").getOrElse("None"))
            .mkString("[", ", ", "]")
        val fallback: Double = sourceNoData.collectFirst { case Some(v) => v }.getOrElse(0.0)
        val fallbackLiteral = f"$fallback%s"

        // Pixel function code is interpolated into VRT XML at PixelCombineRasters-
        // build time. Keep it self-contained — only depends on numpy + the two
        // injected literals (NODATA list, FALLBACK scalar).
        val pythonFunc =
            s"""
               |import numpy as np
               |
               |NODATA = $nodataListLiteral
               |FALLBACK = $fallbackLiteral
               |
               |def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
               |    stacked = np.asarray(in_ar, dtype=np.float64)
               |    valid = np.ones(stacked.shape, dtype=bool)
               |    for i, nd in enumerate(NODATA):
               |        if nd is not None:
               |            valid[i] = stacked[i] != nd
               |    sums = np.where(valid, stacked, 0.0).sum(axis=0)
               |    counts = valid.sum(axis=0)
               |    means = np.where(counts > 0, sums / np.maximum(counts, 1), FALLBACK)
               |    if np.issubdtype(out_ar.dtype, np.integer):
               |        np.copyto(out_ar, np.rint(means), casting='unsafe')
               |    else:
               |        np.copyto(out_ar, means, casting='unsafe')
               |""".stripMargin

        // combineavg PRESERVES band count: averaging K aligned N-band tiles yields an
        // N-band result (output band j = NoData-aware mean of band j across inputs),
        // unlike derivedband which collapses to a single derived band. All inputs are
        // aligned, so the first input's band count is N for the whole stack.
        val bandCount = math.max(rasters.head.GetRasterCount, 1)
        val (resultDs, resultMeta) =
            PixelCombineRasters.combine(rasters, options, pythonFunc, "average", collapseBands = false, bandCount = bandCount)

        // Stamp the chosen NoData onto every output band so callers can detect
        // all-NoData pixels. Only do this when at least one input declared
        // NoData — otherwise we'd invent a sentinel that wasn't present.
        if (sourceNoData.exists(_.isDefined) && resultDs != null) {
            scala.util.Try {
                (1 to resultDs.GetRasterCount).foreach(b => resultDs.GetRasterBand(b).SetNoDataValue(fallback))
                resultDs.FlushCache()
            }
        }

        (resultDs, resultMeta)
    }

}
