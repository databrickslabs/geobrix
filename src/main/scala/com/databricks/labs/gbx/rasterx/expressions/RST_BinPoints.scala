package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.util.{V2Tile, VectorRasterBridge}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference

import scala.collection.mutable

/**
  * The bin-and-reduce kernel behind `gbx_rst_binpoints_agg` ([[RST_BinPointsAgg]]): bin parallel x/y/z points into a
  * single-band Float32 raster tile, reducing the z values of each pixel cell by a statistic.
  *
  * The scalar `gbx_rst_binpoints` has no heavyweight expression: it runs on the lightweight level, which measured
  * faster. This object holds only the shared compute path.
  *
  * Output: single-band Float32 GTiff tile of shape `width_px x height_px` covering
  * `(xmin, ymin) -> (xmax, ymax)` in the given SRID (EPSG code).
  * NoData = -9999.0. Empty cells (no points fall inside them) are filled with NoData.
  *
  * Cell assignment uses a strict half-open interval: a point at exactly `xmax` (or
  * `ymax`) computes `col == width_px` (or `row == height_px`) and is dropped rather
  * than clamped into the last cell. This prevents cross-tile double-counting when
  * data are tiled on shared boundaries.
  *
  * The statistic is one of: `"max"` (default), `"min"`, `"mean"`, `"median"`, `"count"`, or `"percentile:<p>"`
  * (e.g. `"percentile:90"`).
  */
object RST_BinPoints {

    val DefaultStatistic: String = "max"

    // -------------------------------------------------------------------------
    // Pure compute path — Spark-free, direct-execute-friendly
    // -------------------------------------------------------------------------

    /**
      * Bin (x, y, z) point clouds into a `w × h` Float32 raster over
      * `[xmin, xmax) × [ymin, ymax)` and reduce z values per cell by `statistic`.
      *
      * Points on the upper x/y boundary are dropped (strict half-open interval).
      * Empty cells carry NoData (-9999.0). CRS is set from EPSG `srid`.
      *
      * @return GTiff bytes (single-band Float32), or null when x is empty/null.
      */
    def execute(
        x: Array[Double], y: Array[Double], z: Array[Double],
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        w: Int, h: Int, srid: Int,
        statistic: String
    ): Array[Byte] = {
        if (x == null || x.isEmpty) return null
        require(x.length == y.length && x.length == z.length,
            s"gbx_rst_binpoints: x/y/z arrays must have equal length; " +
            s"got ${x.length}/${y.length}/${z.length}")
        require(w > 0, s"gbx_rst_binpoints: width_px must be positive; got $w")
        require(h > 0, s"gbx_rst_binpoints: height_px must be positive; got $h")
        require(xmax > xmin, s"gbx_rst_binpoints: xmax ($xmax) must be > xmin ($xmin)")
        require(ymax > ymin, s"gbx_rst_binpoints: ymax ($ymax) must be > ymin ($ymin)")

        val floats = binAndReduce(x, y, z, xmin, ymin, xmax, ymax, w, h, statistic.toLowerCase)

        // Build a Float32 in-memory raster, write to GTiff bytes, release.
        val memDrv = gdal.GetDriverByName("MEM")
        val ds = memDrv.Create("", w, h, 1, gdalconstConstants.GDT_Float32)
        try {
            val xRes = (xmax - xmin) / w
            val yRes = (ymax - ymin) / h
            // Geotransform: (xmin, xRes, 0, ymax, 0, -yRes) — row 0 = top.
            ds.SetGeoTransform(Array(xmin, xRes, 0.0, ymax, 0.0, -yRes))
            val sr = new SpatialReference()
            try {
                sr.ImportFromEPSG(srid)
                ds.SetProjection(sr.ExportToWkt())
            } finally {
                sr.delete()
            }
            val band = ds.GetRasterBand(1)
            band.SetNoDataValue(-9999.0)
            band.WriteRaster(0, 0, w, h, floats)
            band.FlushCache()
            VectorRasterBridge.toGTiffBytes(ds)
        } finally {
            ds.delete()
        }
    }

    // -------------------------------------------------------------------------
    // Per-cell reduction — mirrors binning.py lines 92–141 exactly
    // -------------------------------------------------------------------------

    /**
      * Assign in-bounds (x, y) to cell indices (half-open interval), accumulate z
      * by `stat`, then return a row-major Float32 array of length `w*h`.
      * Empty cells hold -9999.0f.
      *
      * Matches the Python `binning.py` semantics:
      *   col = floor((x - xmin) / xRange * w)    →  in [0, w)
      *   row = floor((ymax - y) / yRange * h)     →  in [0, h)  (row 0 = top)
      * A point at x == xmax yields col == w → dropped (not clamped).
      * A point with a NaN or infinite x or y is in no pixel and is skipped, as in Python: the test is explicit because
      * `NaN.toInt` is 0 on the JVM, a real pixel.
      */
    private def finite(x: Double, y: Double): Boolean = java.lang.Double.isFinite(x) && java.lang.Double.isFinite(y)

    private def binAndReduce(
        x: Array[Double], y: Array[Double], z: Array[Double],
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        w: Int, h: Int,
        stat: String
    ): Array[Float] = {
        val n      = x.length
        val xRange = xmax - xmin
        val yRange = ymax - ymin

        val result: Array[Double] = stat match {

            case "max" =>
                val acc = Array.fill[Double](w * h)(Double.NaN)
                var i = 0
                while (i < n) {
                    val col = math.floor((x(i) - xmin) / xRange * w).toInt
                    val row = math.floor((ymax - y(i)) / yRange * h).toInt
                    if (finite(x(i), y(i)) && col >= 0 && col < w && row >= 0 && row < h) {
                        val idx = row * w + col
                        acc(idx) = if (acc(idx).isNaN) z(i) else math.max(acc(idx), z(i))
                    }
                    i += 1
                }
                acc

            case "min" =>
                val acc = Array.fill[Double](w * h)(Double.NaN)
                var i = 0
                while (i < n) {
                    val col = math.floor((x(i) - xmin) / xRange * w).toInt
                    val row = math.floor((ymax - y(i)) / yRange * h).toInt
                    if (finite(x(i), y(i)) && col >= 0 && col < w && row >= 0 && row < h) {
                        val idx = row * w + col
                        acc(idx) = if (acc(idx).isNaN) z(i) else math.min(acc(idx), z(i))
                    }
                    i += 1
                }
                acc

            case "mean" =>
                val sums   = new Array[Double](w * h)
                val counts = new Array[Int](w * h)
                var i = 0
                while (i < n) {
                    val col = math.floor((x(i) - xmin) / xRange * w).toInt
                    val row = math.floor((ymax - y(i)) / yRange * h).toInt
                    if (finite(x(i), y(i)) && col >= 0 && col < w && row >= 0 && row < h) {
                        val idx = row * w + col
                        sums(idx)   += z(i)
                        counts(idx) += 1
                    }
                    i += 1
                }
                Array.tabulate[Double](w * h) { idx =>
                    if (counts(idx) > 0) sums(idx) / counts(idx) else Double.NaN
                }

            case "count" =>
                val cnt = new Array[Double](w * h)
                var i = 0
                while (i < n) {
                    val col = math.floor((x(i) - xmin) / xRange * w).toInt
                    val row = math.floor((ymax - y(i)) / yRange * h).toInt
                    if (finite(x(i), y(i)) && col >= 0 && col < w && row >= 0 && row < h) {
                        cnt(row * w + col) += 1.0
                    }
                    i += 1
                }
                // Zero-count cells → NaN so they map to NoData below.
                var j = 0
                while (j < cnt.length) { if (cnt(j) == 0.0) cnt(j) = Double.NaN; j += 1 }
                cnt

            case s if s == "median" || s.startsWith("percentile:") =>
                val p = if (s == "median") 50.0 else s.substring("percentile:".length).toDouble
                val cellVals = mutable.HashMap.empty[Int, mutable.ArrayBuffer[Double]]
                var i = 0
                while (i < n) {
                    val col = math.floor((x(i) - xmin) / xRange * w).toInt
                    val row = math.floor((ymax - y(i)) / yRange * h).toInt
                    if (finite(x(i), y(i)) && col >= 0 && col < w && row >= 0 && row < h) {
                        val idx = row * w + col
                        cellVals.getOrElseUpdate(idx, mutable.ArrayBuffer.empty[Double]) += z(i)
                    }
                    i += 1
                }
                val acc = Array.fill[Double](w * h)(Double.NaN)
                cellVals.foreach { case (idx, vals) =>
                    val sorted = vals.toArray
                    java.util.Arrays.sort(sorted)
                    acc(idx) = percentileLinear(sorted, p)
                }
                acc

            case other =>
                throw new IllegalArgumentException(
                    s"gbx_rst_binpoints: unknown statistic '$other'; " +
                    "expected one of: max, min, mean, median, count, percentile:<p>"
                )
        }

        // Convert to Float32; replace NaN sentinels with the NoData value.
        val floats = new Array[Float](w * h)
        var k = 0
        while (k < floats.length) {
            floats(k) = if (result(k).isNaN) -9999.0f else result(k).toFloat
            k += 1
        }
        floats
    }

    /**
      * Numpy-compatible linear-interpolation percentile.
      *
      * Matches numpy's default `method='linear'`:
      *   idx  = p / 100 * (n - 1)
      *   result = sorted[floor(idx)] + frac * (sorted[ceil(idx)] - sorted[floor(idx)])
      *
      * @param sorted values already sorted ascending.
      * @param p      percentile in [0, 100].
      */
    private def percentileLinear(sorted: Array[Double], p: Double): Double = {
        val n = sorted.length
        if (n == 1) return sorted(0)
        val idx  = p / 100.0 * (n - 1)
        val lo   = math.floor(idx).toInt
        val hi   = math.ceil(idx).toInt
        val frac = idx - lo
        sorted(lo) + frac * (sorted(hi) - sorted(lo))
    }

    // -------------------------------------------------------------------------
    // Tile row assembly
    // -------------------------------------------------------------------------

    /** Assemble a v2 tile InternalRow from GTiff bytes. */
    def tileRow(bytes: Array[Byte]): InternalRow = {
        val mtd = Map(
            "driver"       -> "GTiff",
            "extension"    -> "tif",
            "size"         -> bytes.length.toString,
            "parentPath"   -> "",
            "all_parents"  -> "",
            "last_command" -> "rst_binpoints"
        )
        V2Tile.row(cellid = 0L, raster = bytes, metadata = SerializationUtil.toMapData[String, String](mtd))
    }

}
