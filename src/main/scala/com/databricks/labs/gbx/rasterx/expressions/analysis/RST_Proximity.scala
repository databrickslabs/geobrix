package com.databricks.labs.gbx.rasterx.expressions.analysis

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.ExactDistance
import com.databricks.labs.gbx.rasterx.operator.OperatorOptions
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Coalesce, Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants

/**
  * Compute a proximity raster: each output pixel holds the distance to the
  * nearest non-NoData (or matching `target_values`) source pixel.
  *
  * Computes the exact Euclidean distance (a Felzenszwalb-Huttenlocher transform, [[ExactDistance]]), the same
  * answer as the lightweight tier; `gdal.ComputeProximity` is approximate (up to ~6% off). The output raster has the same extent, CRS,
  * and GeoTransform as the source; pixel dtype is Float32. Distances are
  * measured in pixels (`distunits = "PIXEL"`) or in CRS ground units
  * (`distunits = "GEO"`, default).
  *
  *   - `target_values`: optional comma-separated list of source-pixel values
  *     to measure distance to. When `null`, GDAL treats any non-NoData pixel
  *     as a target.
  *   - `distunits` (default `"GEO"`): `"GEO"` (CRS units) or `"PIXEL"`.
  *   - `max_distance` (default `null` = unlimited): cap distances at this
  *     value; pixels beyond it get the NoData value of the output.
  *
  * Typical uses: distance-to-coast / road / building rasters, cost-surface
  * pre-processing, watershed buffer maps.
  */
case class RST_Proximity(
    tile: Expression,
    targetValuesExpr: Expression,
    distUnitsExpr: Expression,
    maxDistanceExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        tile, targetValuesExpr, distUnitsExpr, maxDistanceExpr, ExpressionConfigExpr()
    )
    // Pin types: target_values String (nullable), distunits String, max_distance Double (nullable).
    override def inputTypes: Seq[DataType] = Seq(
        tile.dataType, StringType, StringType, DoubleType, StringType
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Proximity.name
    // propagateNull=false: builder() injects Literal(null, ...) defaults for the optional args
    // (target_values, max_distance), so a null there must NOT short-circuit the whole result to null
    // (eval must run). runDispatch below guards a null primary tile row so null-tile→null holds.
    override def replacement: Expression = invoke(RST_Proximity, propagateNull = false)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3))

}

object RST_Proximity extends WithExpressionInfo {

    def eval(
        row: InternalRow, targetValues: UTF8String, distUnits: UTF8String,
        maxDistance: Any, conf: UTF8String
    ): InternalRow = runDispatch(row, targetValues, distUnits, maxDistance, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, targetValues: UTF8String, distUnits: UTF8String,
        maxDistance: Any, conf: UTF8String, dt: DataType
    ): InternalRow =
        // With propagateNull=false the invoke now runs eval even for a null primary tile; preserve
        // the prior null-tile→null behavior (rowToTile/safeEval would otherwise NPE on a null row).
        if (row == null) null
        else {
            // A wrong argument fails the call (naming the value); only the tile's own failures become its error tile.
            val unitsArg = Option(distUnits).map(_.toString).getOrElse("GEO")
            requireUnits(unitsArg)
            Option(targetValues).map(_.toString).foreach(parseTargets) // a bad value fails the call, naming it
            requireMaxDistance(maxDistance match {
                case d: Double => Some(d)
                case n: Number => Some(n.doubleValue())
                case _         => None
            })
            safeRun(row, targetValues, distUnits, maxDistance, conf, dt)
        }

    private def safeRun(
        row: InternalRow, targetValues: UTF8String, distUnits: UTF8String,
        maxDistance: Any, conf: UTF8String, dt: DataType
    ): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val tvOpt = Option(targetValues).map(_.toString)
              val unitsStr = Option(distUnits).map(_.toString).getOrElse("GEO")
              // NaN is the "absent" sentinel for the optional max_distance (see builder): drop it to
              // None so execute() treats it as unlimited rather than failing its `> 0 && !NaN` check.
              val maxDistOpt = (maxDistance match {
                  case d: Double => Some(d)
                  case n: Number => Some(n.doubleValue())
                  case _         => None
              }).filterNot(_.isNaN)
              val (resDs, resMtd) = execute(ds, options, tvOpt, unitsStr, maxDistOpt)
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    private def requireUnits(distUnits: String): Unit =
        require(distUnits == "GEO" || distUnits == "PIXEL",
            s"gbx_rst_proximity: distunits must be 'GEO' or 'PIXEL'; got '$distUnits'")

    /** NaN is the "absent" sentinel (unlimited), so only a real value is checked. */
    private def requireMaxDistance(maxDistance: Option[Double]): Unit =
        maxDistance.filterNot(_.isNaN).foreach { d =>
            require(d > 0.0 && !d.isInfinity,
                s"gbx_rst_proximity: max_distance must be > 0 and finite; got $d")
        }

    /** Pure compute path — extracted for direct unit-testing without Spark.
      *
      * Creates a Float32 GTiff at the same extent/CRS as `ds`, then fills it with the exact distance to the
      * nearest source pixel. The output's NoData
      * value is set to -1.0 so unreachable pixels (beyond `maxDistance`) are
      * distinguishable from zero-distance pixels.
      */
    def execute(
        ds: Dataset, options: Map[String, String],
        targetValues: Option[String], distUnits: String, maxDistance: Option[Double]
    ): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_Proximity.execute: source Dataset is null")
        requireUnits(distUnits)
        requireMaxDistance(maxDistance)
        // parsed before the output dataset exists: a bad value fails the call without leaving that dataset behind
        val targets: Option[Set[Long]] = targetValues.filter(_.trim.nonEmpty).map(parseTargets)

        // Build an output GTiff Dataset matching the source's georeferencing.
        val w = ds.GetRasterXSize
        val h = ds.GetRasterYSize
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val outPath = s"/vsimem/proximity_$uuid.tif"
        val driver = gdal.GetDriverByName("GTiff")
        val outDs = driver.Create(outPath, w, h, 1, gdalconstConstants.GDT_Float32)
        // Copy georeferencing (GeoTransform + SRS).
        val gt = ds.GetGeoTransform
        if (gt != null) outDs.SetGeoTransform(gt)
        val srs = ds.GetProjection
        if (srs != null && srs.nonEmpty) outDs.SetProjection(srs)
        val outBand = outDs.GetRasterBand(1)
        outBand.SetNoDataValue(-1.0)

        // Exact Euclidean distance, as the lightweight tier computes it (gdal.ComputeProximity is approximate, up to
        // ~6% off). GDAL compares targets in the integer domain (a pixel is rounded half away from zero to a
        // 32-bit integer first), so the same rounding picks the source pixels here.
        val srcBand = ds.GetRasterBand(1)
        val rowBuf = new Array[Double](w)
        val (xSpacing, ySpacing) =
            if (distUnits == "GEO" && gt != null && gt(1) != 0.0 && gt(5) != 0.0) (math.abs(gt(1)), math.abs(gt(5)))
            else (1.0, 1.0)
        var anySource = false
        val squared = ExactDistance.squaredDistances(w, h, xSpacing, ySpacing, (y, flags) => {
            val rc = srcBand.ReadRaster(0, y, w, 1, rowBuf)
            if (rc != 0) {
                outDs.delete()
                throw new RuntimeException(s"gbx_rst_proximity: reading row $y failed (rc=$rc): ${gdal.GetLastErrorMsg()}")
            }
            var x = 0
            while (x < w) {
                val r = roundHalfAway(rowBuf(x))
                flags(x) = targets.map(_.contains(r)).getOrElse(r != 0L)
                if (flags(x)) anySource = true
                x += 1
            }
        })
        val unlimited = maxDistance.isEmpty
        val cap = maxDistance.map(_.toFloat).getOrElse(0f)
        val result = new Array[Float](w * h)
        var i = 0
        while (i < result.length) {
            val dist = if (anySource) math.sqrt(squared(i)).toFloat else -1.0f
            result(i) = if (!anySource || (!unlimited && dist > cap)) -1.0f else dist
            i += 1
        }
        outBand.WriteRaster(0, 0, w, h, result)
        val errMsg = gdal.GetLastErrorMsg()

        val metadata = Map(
            "path" -> outPath,
            "driver" -> "GTiff",
            "extension" -> "tif",
            "last_command" -> s"ExactDistance(distunits=$distUnits)",
            "last_error" -> (if (errMsg == null) "" else errMsg),
            "all_parents" -> Option(ds.GetDescription()).getOrElse(""),
            "size" -> "-1",
            "format" -> "GTiff",
            "compression" -> options.getOrElse("compression", OperatorOptions.DefaultCompression),
            "isZipped" -> "false",
            "isSubset" -> "false"
        )
        (outDs, metadata)
    }

    /** The integer GDAL compares a pixel as: rounded half away from zero (NaN reads as 0), clamped to a 32-bit int. */
    private def roundHalfAway(v: Double): Long =
        if (v.isNaN) 0L
        else {
            val r = if (v < 0) -math.floor(-v + 0.5) else math.floor(v + 0.5)
            math.max(Int.MinValue.toDouble, math.min(Int.MaxValue.toDouble, r)).toLong
        }

    /** The values of a comma-separated `target_values`, each rounded to the nearest integer (ties to even, as the
      * lightweight tier reads them); a bad entry fails the call, naming the argument. */
    private def parseTargets(text: String): Set[Long] =
        text.split(",").map(_.trim).filter(_.nonEmpty).map { t =>
            try math.rint(t.toDouble).toLong
            catch {
                case _: NumberFormatException =>
                    throw new IllegalArgumentException(
                        s"gbx_rst_proximity: target_values must be comma-separated numbers; got '$text'")
            }
        }.toSet

    override def name: String = "gbx_rst_proximity"

    // max_distance default is NaN, NOT Literal(null, DoubleType): a null primitive-Double arg is
    // force-null-checked by Spark's Invoke regardless of propagateNull=false (it short-circuits the
    // whole result to null before eval runs). A non-null NaN sentinel is never null-checked; eval
    // maps NaN -> None (unlimited). target_values stays StringType (object) so its null is fine.
    private val nanDist: Literal = Literal(Double.NaN, DoubleType)
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 1 => RST_Proximity(c(0), Literal(null, StringType), Literal("GEO"), nanDist)
        case 2 => RST_Proximity(c(0), c(1), Literal("GEO"), nanDist)
        case 3 => RST_Proximity(c(0), c(1), c(2), nanDist)
        // a NULL max_distance (the SQL default of the Python-registered function) means unlimited, like the NaN default
        case 4 => RST_Proximity(c(0), c(1), c(2), Coalesce(Seq(c(3), nanDist)))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_proximity takes 1 to 4 arguments (tile, [target_values, [distunits, [max_distance]]]); got $n"
        )
    }

}
