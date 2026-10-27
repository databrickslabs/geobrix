package com.databricks.labs.gbx.rasterx.expressions.pixel

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver, ValidMask}
import com.databricks.labs.gbx.rasterx.operations.BandAccessors
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.gdal.gdalconst.gdalconstConstants.GMF_PER_DATASET

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
    // propagateNull=false: builder() injects Literal(null, DoubleType) defaults for the optional
    // min_val/max_val, so a null there must NOT short-circuit the whole result to null (eval must
    // run). doInvoke below guards a null primary tile row so the prior null-tile→null behavior holds.
    override def replacement: Expression = invoke(RST_Histogram, propagateNull = false)
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
        // With propagateNull=false the invoke now runs eval even for a null primary tile; preserve
        // the prior null-tile→null behavior (rowToDS/safeEval would otherwise NPE on a null row).
        if (row == null) null
        else Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, dt)
                // NaN is the "absent" sentinel for the optional min/max (see builder): treat it, like
                // a genuine null, as "derive from band statistics".
                val minOpt = Option(minVal).map(_.doubleValue()).filterNot(_.isNaN)
                val maxOpt = Option(maxVal).map(_.doubleValue()).filterNot(_.isNaN)
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
        // A raster with an alpha band beside data bands: the transparent pixels are not valid for a band and
        // are not counted in it (GDAL's range and histogram count them at every band count but the 2- and
        // 4-band ones). A band masked by its own mask band is already left out by GDAL.
        val gated = GDAL.hasAlphaBesideData(ds)
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
                    if (gated) {
                        val st = BandAccessors.validStats(ds, b)
                        if (st.count > 0L) { stats(0) = st.min; stats(1) = st.max }
                    } else band.ComputeRasterMinMax(stats, 1) // 1 = approx ok
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
            val counts = new Array[Long](nBuckets)
            // GetHistogram signature (Java binding):
            //   int GetHistogram(double min, double max, int[] panHistogram,
            //                    boolean bIncludeOutOfRange, boolean bApproxOK)
            if (gated) validHistogram(ds, b, loEff, hiEff, counts)
            else {
                val gdalCounts = new Array[Int](nBuckets)
                band.GetHistogram(loEff, hiEff, gdalCounts, false, false)
                // Widen to Long for the MAP<STRING, ARRAY<LONG>> return shape.
                var i = 0
                while (i < nBuckets) { counts(i) = gdalCounts(i).toLong; i += 1 }
            }
            java.lang.System.arraycopy(counts, 0, buckets, 0, nBuckets)
            result += (s"band_$b" -> buckets.clone())
            b += 1
        }
        result.toMap
    }

    /**
      * Adds to `counts` GDAL's histogram of the pixels of band `b` of `ds` that are valid for it
      * ([[ValidMask.readBand]]). The band is copied, strip by strip, into a new in-memory dataset of the same data
      * type whose mask band is that validity, and `GetHistogram` runs on each strip with the same range and
      * buckets, so the bucket rule is GDAL's own and the counts of the strips add up to those of the band.
      * Memory: one strip (at most [[ValidMask.StripPixels]] pixels: the copy, its mask, and the Double and Byte
      * read buffers), whatever the size of the band. (A strip has its own dataset: GDAL's block cache of a reused
      * in-memory dataset serves the previous strip's blocks to the next histogram.)
      */
    private def validHistogram(ds: Dataset, b: Int, lo: Double, hi: Double, counts: Array[Long]): Unit = {
        val (xSize, ySize) = (ds.GetRasterXSize(), ds.GetRasterYSize())
        val src            = ds.GetRasterBand(b)
        val rows           = math.max(1, math.min(ySize, ValidMask.StripPixels / math.max(1, xSize)))
        val data           = new Array[Double](xSize * rows)
        val ok             = new Array[Byte](xSize * rows)
        val part           = new Array[Int](counts.length)
        val validity       = new ValidMask(ds)
        var y0 = 0
        while (y0 < ySize) {
            val h    = math.min(rows, ySize - y0)
            val what = s"band $b, rows $y0..${y0 + h - 1}"
            GDAL.requireOk(src.ReadRaster(0, y0, xSize, h, data), s"reading $what")
            validity.readBand(b, 0, y0, xSize, h, ok)
            val strip = GDALManager.memDriver().Create("", xSize, h, 1, src.getDataType)
            try {
                GDAL.requireOk(strip.CreateMaskBand(GMF_PER_DATASET), s"creating the mask band of the histogram of $what")
                val dst = strip.GetRasterBand(1)
                GDAL.requireOk(dst.WriteRaster(0, 0, xSize, h, data), s"copying $what")
                GDAL.requireOk(dst.GetMaskBand().WriteRaster(0, 0, xSize, h, ok), s"copying the validity of $what")
                java.util.Arrays.fill(part, 0)
                GDAL.requireOk(dst.GetHistogram(lo, hi, part, false, false), s"the histogram of $what")
            } finally strip.delete()
            var i = 0
            while (i < part.length) { counts(i) += part(i).toLong; i += 1 }
            y0 += h
        }
    }

    override def name: String = "gbx_rst_histogram"

    // min/max default is NaN, NOT Literal(null, DoubleType): a null primitive-Double arg is
    // force-null-checked by Spark's Invoke regardless of propagateNull=false (it short-circuits the
    // whole result to null before eval runs, which silently broke rst_histogram(tile)). A non-null
    // NaN sentinel is never null-checked; eval treats NaN like null -> derive range from band stats.
    private def nanDouble: Literal = Literal(Double.NaN, DoubleType)

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 1 => RST_Histogram(c(0), Literal(256), nanDouble, nanDouble, Literal(false))
        case 2 => RST_Histogram(c(0), c(1), nanDouble, nanDouble, Literal(false))
        case 3 => RST_Histogram(c(0), c(1), c(2), nanDouble, Literal(false))
        case 4 => RST_Histogram(c(0), c(1), c(2), c(3), Literal(false))
        case 5 => RST_Histogram(c(0), c(1), c(2), c(3), c(4))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_histogram takes 1 to 5 arguments (tile, [n_buckets, [min_val, [max_val, [include_nodata]]]]); got $n"
        )
    }

}
