package com.databricks.labs.gbx.rasterx.expressions.analysis

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.ogr.{FieldDefn, ogr}
import org.gdal.ogr.ogrConstants.{OFTReal, wkbLineString}

import java.util.{Vector => JVector}
import scala.collection.mutable.ArrayBuffer

/**
  * Generate contour lines from a raster as an array of `(geom_wkb, value)`
  * features.
  *
  * Wraps `gdal.ContourGenerateEx`. Either supplies a single equal-interval
  * (`levelInterval`) — every `interval` step produces a contour at
  * `base + n*interval` — OR a fixed list of contour values via `levels`.
  *
  *   - `levels` (`ARRAY<DOUBLE>`): explicit contour values (FIXED_LEVELS).
  *     Pass an empty array to use `interval` instead.
  *   - `interval` (`DOUBLE`): step between contours; ignored if `levels` is
  *     non-empty.
  *   - `base` (default `0.0`): contour base value — only meaningful with
  *     `interval`. Contours appear at `base + n*interval`.
  *   - `attr_field` (default `"elev"`): name of the OGR field that carries
  *     each contour's value. Read back via the `value` member of the output
  *     struct; the field name is purely an internal label.
  *
  * Output: `ARRAY<struct(geom_wkb BINARY, value DOUBLE)>` — one entry per
  * contour LineString. Geometry is WKB in the raster's CRS.
  */
case class RST_Contour(
    tile: Expression,
    levelsExpr: Expression,
    intervalExpr: Expression,
    baseExpr: Expression,
    attrFieldExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] =
        Seq(tile, levelsExpr, intervalExpr, baseExpr, attrFieldExpr, ExpressionConfigExpr())
    // Pin types — levels is ARRAY<DOUBLE>, interval/base Double, attr_field String.
    override def inputTypes: Seq[DataType] = Seq(
        tile.dataType, ArrayType(DoubleType), DoubleType, DoubleType, StringType, StringType
    )
    override def dataType: DataType = ArrayType(
        StructType(Seq(
            StructField("geom_wkb", BinaryType),
            StructField("value", DoubleType)
        ))
    )
    override def nullable: Boolean = true
    override def prettyName: String = RST_Contour.name
    override def replacement: Expression = invoke(RST_Contour)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4))

}

object RST_Contour extends WithExpressionInfo {

    def eval(
        row: InternalRow, levels: ArrayData, interval: Double, base: Double,
        attrField: UTF8String, conf: UTF8String
    ): ArrayData = doInvoke(row, levels, interval, base, attrField, conf, BinaryType)

    private def doInvoke(
        row: InternalRow, levels: ArrayData, interval: Double, base: Double,
        attrField: UTF8String, conf: UTF8String, rdt: DataType
    ): ArrayData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, rdt)
                val lvls = if (levels == null) Array.empty[Double] else levels.toDoubleArray()
                val attr = Option(attrField).map(_.toString).getOrElse("elev")
                try execute(ds, lvls, interval, base, attr)
                finally RasterDriver.releaseDataset(ds)
            },
            row,
            rdt,
            conf
          )
        ).map(_.asInstanceOf[ArrayData]).orNull

    /** Pure compute path — extracted for direct unit-testing without Spark.
      *
      * Runs `gdal.ContourGenerateEx(band, outLayer, options)` and returns each
      * LineString as `(WKB, value)`. The output layer's CRS inherits from the
      * source raster.
      */
    def execute(
        ds: Dataset, levels: Array[Double], interval: Double, base: Double, attrField: String
    ): ArrayData = {
        require(ds != null, "RST_Contour.execute: source Dataset is null")
        // Either levels is non-empty, or interval must be positive.
        if (levels.isEmpty) {
            require(interval > 0.0 && !interval.isNaN && !interval.isInfinity,
                s"gbx_rst_contour: levels is empty so interval must be > 0 and finite; got $interval")
        }
        require(attrField != null && attrField.nonEmpty,
            "gbx_rst_contour: attr_field must be non-empty")

        GDALManager.initOgr()
        val ogrDriver = ogr.GetDriverByName("Memory")
        val outDs = ogrDriver.CreateDataSource("rst_contour_out")
        val srcSrs = ds.GetSpatialRef
        val outLayer = outDs.CreateLayer("contours", srcSrs, wkbLineString)
        val fd = new FieldDefn(attrField, OFTReal)
        outLayer.CreateField(fd); fd.delete()
        // Find the field index just created (always 0 in a fresh layer).
        val fieldIdx = outLayer.GetLayerDefn().GetFieldIndex(attrField)

        // Build ContourGenerateEx options — see GDAL docs for the option set.
        val opts = new JVector[String]()
        opts.add(s"ID_FIELD=-1")
        opts.add(s"ELEV_FIELD=$fieldIdx")
        if (levels.nonEmpty) {
            opts.add(s"FIXED_LEVELS=${levels.mkString(",")}")
        } else {
            opts.add(s"LEVEL_INTERVAL=$interval")
            if (base != 0.0) opts.add(s"LEVEL_BASE=$base")
        }

        val srcBand = ds.GetRasterBand(1)
        val rc = gdal.ContourGenerateEx(srcBand, outLayer, opts)
        if (rc != 0) {
            val errMsg = gdal.GetLastErrorMsg()
            outDs.delete()
            throw new RuntimeException(
                s"gbx_rst_contour: gdal.ContourGenerateEx failed (rc=$rc): " +
                  (if (errMsg == null || errMsg.isEmpty) "<no error>" else errMsg)
            )
        }

        try {
            outLayer.ResetReading()
            val rows = ArrayBuffer.empty[InternalRow]
            var feat = outLayer.GetNextFeature()
            while (feat != null) {
                val geom = feat.GetGeometryRef()
                if (geom != null) {
                    val wkb = geom.ExportToWkb()
                    val v = feat.GetFieldAsDouble(fieldIdx)
                    rows += InternalRow.fromSeq(Seq(wkb, v))
                }
                feat.delete()
                feat = outLayer.GetNextFeature()
            }
            new GenericArrayData(rows.toArray[Any])
        } finally {
            outDs.delete()
        }
    }

    override def name: String = "gbx_rst_contour"

    /** Builder: tile + (levels OR interval), optional base, optional attr_field.
      *
      * `levels` is `ARRAY<DOUBLE>` for explicit contour values; pass `array()`
      * (empty) to fall back to `interval`. Sentinel for "no fixed levels" is
      * an empty array literal — keeps Catalyst typing tidy.
      */
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => RST_Contour(c(0), c(1), Literal(0.0), Literal(0.0), Literal("elev"))
        case 3 => RST_Contour(c(0), c(1), c(2), Literal(0.0), Literal("elev"))
        case 4 => RST_Contour(c(0), c(1), c(2), c(3), Literal("elev"))
        case 5 => RST_Contour(c(0), c(1), c(2), c(3), c(4))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_contour takes 2 to 5 arguments (tile, levels, [interval, [base, [attr_field]]]); got $n"
        )
    }

}
