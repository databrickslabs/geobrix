package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.util.RasterArgumentException
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference

/**
  * Grid-alignment precondition guard for the cross-raster combine family
  * (`gbx_rst_combine{min,max,median,sum,stddev,count}`).
  *
  * These reducers evaluate a per-pixel statistic across a stack of tiles by stacking
  * their bands into a single `-separate` VRT and applying a Python pixel function. That
  * pixel-index correspondence is only meaningful when every input shares the SAME grid —
  * identical CRS, extent, pixel size and dimensions. `gdalbuildvrt` will happily build a
  * VRT over MISALIGNED inputs (covering their union extent and snapping to the highest
  * resolution), which silently produces a statistically meaningless result. So the combine
  * family declares alignment as a hard precondition and errors clearly when it is violated,
  * pointing the caller at `gbx_rst_align_to` as the explicit fix.
  */
object RasterAlignment {

    /** Relative tolerance for comparing geotransform coefficients (floating-point origins/pixel sizes). */
    private val RelTol = 1e-6

    private def approxEq(a: Double, b: Double): Boolean =
        math.abs(a - b) <= RelTol * (1.0 + math.abs(b))

    /** True when two datasets share the same CRS. Both CRS-less counts as same (grid-native assumption). */
    private def sameCrs(a: Dataset, b: Dataset): Boolean = {
        val wa = a.GetProjection()
        val wb = b.GetProjection()
        val ea = wa == null || wa.isEmpty
        val eb = wb == null || wb.isEmpty
        if (ea && eb) true
        else if (ea != eb) false
        else {
            val sa = new SpatialReference(); sa.ImportFromWkt(wa)
            val sb = new SpatialReference(); sb.ImportFromWkt(wb)
            val same = sa.IsSame(sb) == 1
            sa.delete(); sb.delete()
            same
        }
    }

    /** True when `a` and `b` share identical dimensions, geotransform (within tolerance), and CRS. */
    def isAligned(a: Dataset, b: Dataset): Boolean = {
        if (a.GetRasterXSize != b.GetRasterXSize) return false
        if (a.GetRasterYSize != b.GetRasterYSize) return false
        val ga = Array.ofDim[Double](6); a.GetGeoTransform(ga)
        val gb = Array.ofDim[Double](6); b.GetGeoTransform(gb)
        if (!ga.indices.forall(i => approxEq(ga(i), gb(i)))) return false
        sameCrs(a, b)
    }

    /** One-line grid summary used in the misalignment error message. */
    private def describe(ds: Dataset): String = {
        val gt = Array.ofDim[Double](6); ds.GetGeoTransform(gt)
        val crs = Option(ds.GetProjection()).filter(_.nonEmpty).map { wkt =>
            val sr = new SpatialReference(); sr.ImportFromWkt(wkt)
            val name = Option(sr.GetAuthorityName(null))
            val code = Option(sr.GetAuthorityCode(null))
            sr.delete()
            (name, code) match {
                case (Some(n), Some(c)) if n.nonEmpty && c.nonEmpty => s"$n:$c"
                case _                                              => "custom"
            }
        }.getOrElse("none")
        s"${ds.GetRasterXSize}x${ds.GetRasterYSize}px, origin=(${gt(0)}, ${gt(3)}), " +
            s"pixel=(${gt(1)}, ${gt(5)}), crs=$crs"
    }

    /**
      * Require every dataset in `rasters` to share tile 0's grid, else throw a
      * [[RasterArgumentException]] naming the first offending tile and pointing at
      * `gbx_rst_align_to`. A stack of 0 or 1 tiles is trivially aligned.
      */
    def requireAligned(rasters: Seq[Dataset], funcName: String): Unit = {
        if (rasters.length <= 1) return
        val ref = rasters.head
        rasters.zipWithIndex.tail.foreach { case (ds, i) =>
            if (!isAligned(ref, ds)) {
                throw new RasterArgumentException(
                    s"$funcName requires all input tiles to share an identical grid " +
                        s"(CRS, extent, pixel size, and dimensions), but tile $i does not match tile 0. " +
                        s"tile 0: ${describe(ref)}; tile $i: ${describe(ds)}. Cross-raster combine does NOT " +
                        s"resample or reproject its inputs — align them first with " +
                        s"gbx_rst_align_to(tile, reference_tile).")
            }
        }
    }

}
