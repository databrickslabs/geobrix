package com.databricks.labs.gbx.rasterx.expressions.pixel

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/**
  * Per-band pixel histogram via `band.GetHistogram(min, max, buckets, ...)`.
  *
  * Returns `MAP<STRING, ARRAY<LONG>>` keyed by ``"band_<i>"`` (1-based) with a
  * length-`n_buckets` array of bucket counts per band. Pixels with values
  * outside `[min_val, max_val]` are dropped (no out-of-range bucket).
  *
  *   - `n_buckets` (default 256): number of equal-width buckets across `[min_val, max_val]`.
  *   - `min_val` / `max_val` (defaults: derived from band statistics if null): explicit
  *     histogram range. Passing both lets the caller align histograms across
  *     tiles for comparable distributions.
  *   - `include_nodata` (default false): currently ignored — GDAL excludes
  *     NoData from the histogram regardless. Kept on the signature for future
  *     symmetry with `gdal_histogram`'s `--no_data` flag.
  */
case class RST_Histogram(
    tile: Expression,
    nBucketsExpr: Expression,
    minValExpr: Expression,
    maxValExpr: Expression,
    includeNodataExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        tile, nBucketsExpr, minValExpr, maxValExpr, includeNodataExpr, ExpressionConfigExpr()
    )
    // Pin n_buckets as IntegerType, min/max as DoubleType, include_nodata as BooleanType
    // so SQL literals (e.g. `null`, `5.0`, `false`) coerce cleanly.
    override def inputTypes: Seq[DataType] = Seq(
        tile.dataType, IntegerType, DoubleType, DoubleType, BooleanType, StringType
    )
    override def dataType: DataType = MapType(StringType, ArrayType(LongType))
    override def nullable: Boolean = true
    override def prettyName: String = RST_Histogram.name
    override def replacement: Expression = invoke(RST_Histogram)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4))

}

object RST_Histogram extends WithExpressionInfo {

    def eval(
        row: InternalRow,
        nBuckets: Int, minVal: java.lang.Double, maxVal: java.lang.Double,
        includeNodata: Boolean, conf: UTF8String
    ): MapData = doInvoke(row, nBuckets, minVal, maxVal, includeNodata, conf, BinaryType)
    // PySpark commonly serialises integer literals as Long.
    def eval (
        row: InternalRow,
        nBuckets: Long, minVal: java.lang.Double, maxVal: java.lang.Double,
        includeNodata: Boolean, conf: UTF8String
    ): MapData = doInvoke(row, nBuckets.toInt, minVal, maxVal, includeNodata, conf, BinaryType)

    private def doInvoke(
        row: InternalRow,
        nBuckets: Int, minVal: java.lang.Double, maxVal: java.lang.Double,
        includeNodata: Boolean, conf: UTF8String, dt: DataType
    ): MapData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, dt)
                val minOpt = if (minVal == null) None else Some(minVal.doubleValue())
                val maxOpt = if (maxVal == null) None else Some(maxVal.doubleValue())
                val hist = execute(ds, nBuckets, minOpt, maxOpt, includeNodata)
                RasterDriver.releaseDataset(ds)
                // Build MapData manually because the values are Array[Long].
                val keys = new GenericArrayData(hist.keys.toArray.map(k => UTF8String.fromString(k)))
                val values = new GenericArrayData(
                    hist.values.toArray.map(v => new GenericArrayData(v.map(java.lang.Long.valueOf)))
                )
                new ArrayBasedMapData(keys, values)
            },
            row,
            dt,
            conf
          )
        ).map(_.asInstanceOf[MapData]).orNull

    /** Pure compute path — extracted for direct unit-testing without Spark.
      *
      * `minOpt` / `maxOpt` default to the band's `[min_val, max_val]` via
      * `band.GetMinimum / GetMaximum` (with a `ComputeStatistics` fallback).
      */
    def execute(
        ds: Dataset, nBuckets: Int,
        minOpt: Option[Double], maxOpt: Option[Double],
        includeNodata: Boolean
    ): Map[String, Array[Long]] = {
        require(ds != null, "RST_Histogram.execute: source Dataset is null")
        require(nBuckets >= 1, s"gbx_rst_histogram: n_buckets must be >= 1; got $nBuckets")
        val _ = includeNodata // currently advisory only
        val nBands = ds.GetRasterCount
        val buckets = new Array[Long](nBuckets) // reused (overwritten per band)
        val result = scala.collection.mutable.LinkedHashMap.empty[String, Array[Long]]
        var b = 1
        while (b <= nBands) {
            val band = ds.GetRasterBand(b)
            // Get min/max — caller-supplied takes precedence; otherwise derive
            // from the band. Note: GetMinimum / GetMaximum return null until
            // ComputeStatistics has been run.
            val (lo, hi) = (minOpt, maxOpt) match {
                case (Some(a), Some(c)) => (a, c)
                case _ =>
                    val stats = new Array[Double](2)
                    band.ComputeRasterMinMax(stats, 1) // 1 = approx ok
                    (
                        minOpt.getOrElse(stats(0)),
                        maxOpt.getOrElse(stats(1))
                    )
            }
            // GDAL's GetHistogram requires hi > lo; if the raster is constant
            // we pad the range by a small epsilon so all pixels land in bucket 0.
            val (loEff, hiEff) =
                if (hi > lo) (lo, hi)
                else {
                    val eps = if (lo == 0.0) 1.0 else math.abs(lo) * 1e-9 + 1e-12
                    (lo, lo + eps)
                }
            val counts = new Array[Int](nBuckets)
            // GetHistogram signature (Java binding):
            //   int GetHistogram(double min, double max, int[] panHistogram,
            //                    boolean bIncludeOutOfRange, boolean bApproxOK)
            band.GetHistogram(loEff, hiEff, counts, false, false)
            // Widen to Long for the MAP<STRING, ARRAY<LONG>> return shape.
            var i = 0
            while (i < nBuckets) {
                buckets(i) = counts(i).toLong
                i += 1
            }
            result += (s"band_$b" -> buckets.clone())
            b += 1
        }
        result.toMap
    }

    override def name: String = "gbx_rst_histogram"

    /** Build a Literal that boxes a Java Double null — needed so the optional
      * min/max can be passed through SQL `null` literals without a coercion error. */
    private def nullDouble: Literal = Literal.create(null, DoubleType)

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 1 => RST_Histogram(c(0), Literal(256), nullDouble, nullDouble, Literal(false))
        case 2 => RST_Histogram(c(0), c(1), nullDouble, nullDouble, Literal(false))
        case 3 => RST_Histogram(c(0), c(1), c(2), nullDouble, Literal(false))
        case 4 => RST_Histogram(c(0), c(1), c(2), c(3), Literal(false))
        case 5 => RST_Histogram(c(0), c(1), c(2), c(3), c(4))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_histogram takes 1 to 5 arguments (tile, [n_buckets, [min_val, [max_val, [include_nodata]]]]); got $n"
        )
    }

}
