package com.databricks.labs.gbx.rasterx.expressions.analysis

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants

import java.util.{Vector => JVector}

/**
  * Compute a proximity raster: each output pixel holds the distance to the
  * nearest non-NoData (or matching `target_values`) source pixel.
  *
  * Wraps `gdal.ComputeProximity`. The output raster has the same extent, CRS,
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
    override def replacement: Expression = invoke(RST_Proximity)
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
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val tvOpt = Option(targetValues).map(_.toString)
              val unitsStr = Option(distUnits).map(_.toString).getOrElse("GEO")
              val maxDistOpt = maxDistance match {
                  case null         => None
                  case d: Double    => Some(d)
                  case n: Number    => Some(n.doubleValue())
                  case _            => None
              }
              val (resDs, resMtd) = execute(ds, options, tvOpt, unitsStr, maxDistOpt)
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    /** Pure compute path — extracted for direct unit-testing without Spark.
      *
      * Creates a Float32 GTiff at the same extent/CRS as `ds`, then runs
      * `gdal.ComputeProximity(srcBand, outBand, options)`. The output's NoData
      * value is set to -1.0 so unreachable pixels (beyond `maxDistance`) are
      * distinguishable from zero-distance pixels.
      */
    def execute(
        ds: Dataset, options: Map[String, String],
        targetValues: Option[String], distUnits: String, maxDistance: Option[Double]
    ): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_Proximity.execute: source Dataset is null")
        require(distUnits == "GEO" || distUnits == "PIXEL",
            s"gbx_rst_proximity: distunits must be 'GEO' or 'PIXEL'; got '$distUnits'")
        maxDistance.foreach { d =>
            require(d > 0.0 && !d.isNaN && !d.isInfinity,
                s"gbx_rst_proximity: max_distance must be > 0 and finite; got $d")
        }

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

        val gdalOpts = new JVector[String]()
        gdalOpts.add(s"DISTUNITS=$distUnits")
        gdalOpts.add("NODATA=-1.0")
        targetValues.foreach(tv => gdalOpts.add(s"VALUES=$tv"))
        maxDistance.foreach(d => gdalOpts.add(s"MAXDIST=$d"))

        val srcBand = ds.GetRasterBand(1)
        val rc = gdal.ComputeProximity(srcBand, outBand, gdalOpts)
        if (rc != 0) {
            val errMsg = gdal.GetLastErrorMsg()
            outDs.delete()
            throw new RuntimeException(
                s"gbx_rst_proximity: gdal.ComputeProximity failed (rc=$rc): " +
                  (if (errMsg == null || errMsg.isEmpty) "<no error>" else errMsg)
            )
        }
        outBand.FlushCache()
        outDs.FlushCache()
        val errMsg = gdal.GetLastErrorMsg()

        val metadata = Map(
            "path" -> outPath,
            "driver" -> "GTiff",
            "extension" -> "tif",
            "last_command" -> s"gdal.ComputeProximity(distunits=$distUnits)",
            "last_error" -> (if (errMsg == null) "" else errMsg),
            "all_parents" -> Option(ds.GetDescription()).getOrElse(""),
            "size" -> "-1",
            "format" -> "GTiff",
            "compression" -> "DEFLATE",
            "isZipped" -> "false",
            "isSubset" -> "false"
        )
        (outDs, metadata)
    }

    override def name: String = "gbx_rst_proximity"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 1 => RST_Proximity(c(0), Literal(null, StringType), Literal("GEO"), Literal(null, DoubleType))
        case 2 => RST_Proximity(c(0), c(1), Literal("GEO"), Literal(null, DoubleType))
        case 3 => RST_Proximity(c(0), c(1), c(2), Literal(null, DoubleType))
        case 4 => RST_Proximity(c(0), c(1), c(2), c(3))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_proximity takes 1 to 4 arguments (tile, [target_values, [distunits, [max_distance]]]); got $n"
        )
    }

}
