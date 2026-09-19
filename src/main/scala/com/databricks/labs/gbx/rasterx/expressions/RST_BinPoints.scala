package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, V2Tile, VectorRasterBridge}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference

import scala.collection.mutable

/**
  * Bin parallel x/y/z ARRAY&lt;DOUBLE&gt; columns into a single-band Float32 raster tile,
  * reducing z values per pixel cell by a statistic.
  *
  * This is the non-aggregator (scalar) form — all three arrays live in a single row.
  * For the grouped-agg form (one point per row) use [[RST_BinPointsAgg]].
  *
  * Output: single-band Float32 GTiff tile of shape `width_px × height_px` covering
  * `(xmin, ymin) → (xmax, ymax)` in the given SRID (EPSG code).
  * NoData = -9999.0. Empty cells (no points fall inside them) are filled with NoData.
  *
  * Cell assignment uses a strict half-open interval: a point at exactly `xmax` (or
  * `ymax`) computes `col == width_px` (or `row == height_px`) and is dropped rather
  * than clamped into the last cell. This prevents cross-tile double-counting when
  * data are tiled on shared boundaries.
  *
  * @param statisticExpr one of: `"max"` (default), `"min"`, `"mean"`, `"median"`,
  *                      `"count"`, or `"percentile:&lt;p&gt;"` (e.g. `"percentile:90"`).
  */
case class RST_BinPoints(
    xArrayExpr: Expression,
    yArrayExpr: Expression,
    zArrayExpr: Expression,
    xminExpr: Expression,
    yminExpr: Expression,
    xmaxExpr: Expression,
    ymaxExpr: Expression,
    widthPxExpr: Expression,
    heightPxExpr: Expression,
    sridExpr: Expression,
    statisticExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        xArrayExpr, yArrayExpr, zArrayExpr,
        xminExpr, yminExpr, xmaxExpr, ymaxExpr,
        widthPxExpr, heightPxExpr, sridExpr,
        statisticExpr,
        ExpressionConfigExpr()
    )

    // Pin all inputs to canonical types so Catalyst coerces SQL literals.
    // ARRAY<DECIMAL> → ARRAY<DOUBLE>; INT/DECIMAL scalars → DOUBLE/INT.
    // Without this, inline SQL DECIMAL arrays + integer scalars return null silently
    // (same bug we already hit on gbx_rst_isoband). MANDATORY.
    override def inputTypes: Seq[DataType] = Seq(
        ArrayType(DoubleType), ArrayType(DoubleType), ArrayType(DoubleType),
        DoubleType, DoubleType, DoubleType, DoubleType,
        IntegerType, IntegerType, IntegerType,
        StringType,
        StringType  // ExpressionConfigExpr
    )

    override def dataType: DataType   = RST_ExpressionUtil.tileDataType(BinaryType)
    override def nullable: Boolean    = true
    override def prettyName: String   = RST_BinPoints.name
    override def replacement: Expression = invoke(RST_BinPoints)

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8), nc(9), nc(10))

}

/** Companion: SQL name, eval entry points, pure compute path, builder. */
object RST_BinPoints extends WithExpressionInfo {

    val DefaultStatistic: String = "max"

    // -------------------------------------------------------------------------
    // Catalyst / PySpark eval entry points
    // -------------------------------------------------------------------------

    /** Int-args entry point — used by Catalyst for non-PySpark (SQL) callers. */
    def eval(
        xData: ArrayData, yData: ArrayData, zData: ArrayData,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, srid: Int,
        statistic: UTF8String,
        conf: UTF8String
    ): InternalRow = doInvoke(
        xData, yData, zData, xmin, ymin, xmax, ymax,
        widthPx, heightPx, srid, statistic, conf
    )

    /** Long-args entry point — used by PySpark (Python ints arrive as Long). */
    def eval(
        xData: ArrayData, yData: ArrayData, zData: ArrayData,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Long, heightPx: Long, srid: Long,
        statistic: UTF8String,
        conf: UTF8String
    ): InternalRow = doInvoke(
        xData, yData, zData, xmin, ymin, xmax, ymax,
        widthPx.toInt, heightPx.toInt, srid.toInt, statistic, conf
    )

    private def doInvoke(
        xData: ArrayData, yData: ArrayData, zData: ArrayData,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, srid: Int,
        statistic: UTF8String,
        conf: UTF8String
    ): InternalRow =
        // Empty/null input is a valid case — return null quietly, bypassing safeEval
        // so no NonLocalReturnControl is thrown and no spurious WARN is logged.
        if (xData == null || xData.numElements() == 0) null
        else Option(
            RST_ErrorHandler.safeEval(
                () => {
                    val exprConf = ExpressionConfig.fromB64(conf.toString)
                    RST_ExpressionUtil.init(exprConf)
                    val x     = xData.toDoubleArray()
                    val y     = yData.toDoubleArray()
                    val z     = zData.toDoubleArray()
                    val stat  = if (statistic == null) DefaultStatistic else statistic.toString
                    val bytes = execute(x, y, z, xmin, ymin, xmax, ymax, widthPx, heightPx, srid, stat)
                    // bytes is null only when x.isEmpty, already guarded above; return tileRow.
                    if (bytes == null) null else tileRow(bytes)
                },
                null,
                BinaryType,
                conf
            )
        ).map(_.asInstanceOf[InternalRow]).orNull

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
      */
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
                    if (col >= 0 && col < w && row >= 0 && row < h) {
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
                    if (col >= 0 && col < w && row >= 0 && row < h) {
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
                    if (col >= 0 && col < w && row >= 0 && row < h) {
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
                    if (col >= 0 && col < w && row >= 0 && row < h) {
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
                    if (col >= 0 && col < w && row >= 0 && row < h) {
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

    // -------------------------------------------------------------------------
    // Registration
    // -------------------------------------------------------------------------

    override def name: String = "gbx_rst_binpoints"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 10 => RST_BinPoints(
            c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9),
            Literal(DefaultStatistic)
        )
        case 11 => RST_BinPoints(
            c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10)
        )
        case n  => throw new IllegalArgumentException(
            s"gbx_rst_binpoints takes 10 or 11 arguments " +
            "(x_array, y_array, z_array, xmin, ymin, xmax, ymax, width_px, height_px, srid, [statistic]); " +
            s"got $n"
        )
    }
}
