package com.databricks.labs.gbx.rasterx.expressions.vector

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, VectorRasterBridge}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}

import java.util.{Vector => JVector}

/** Burn a vector geometry into a raster tile at the given extent and resolution.
 *
 *  Returns a GTiff-backed tile of shape `width_px x height_px` covering the
 *  bounding box `(xmin, ymin) -> (xmax, ymax)` in the given SRID. Pixels inside
 *  the geometry get the burn `value`; pixels outside get the NoData sentinel
 *  (-9999.0, Float64).
 */
case class RST_Rasterize(
    geomWkbExpr: Expression,
    valueExpr: Expression,
    xminExpr: Expression,
    yminExpr: Expression,
    xmaxExpr: Expression,
    ymaxExpr: Expression,
    widthPxExpr: Expression,
    heightPxExpr: Expression,
    sridExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        geomWkbExpr, valueExpr,
        xminExpr, yminExpr, xmaxExpr, ymaxExpr,
        widthPxExpr, heightPxExpr, sridExpr,
        ExpressionConfigExpr()
    )
    // Pin the numeric arg types so ImplicitCastInputTypes coerces SQL decimal literals
    // (e.g. ``42.0``) to ``Double`` and SQL int literals to ``Int`` before catalyst's
    // reflective method lookup — otherwise the dispatcher receives ``Decimal`` and the
    // ``def eval(... Double ... Int ...)`` overload is not found.
    override def inputTypes: Seq[DataType] = Seq(
        BinaryType, DoubleType,
        DoubleType, DoubleType, DoubleType, DoubleType,
        IntegerType, IntegerType, IntegerType,
        StringType
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(BinaryType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Rasterize.name
    override def replacement: Expression = invoke(RST_Rasterize)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8))

}

/** Companion: SQL name, builder, and entry points for catalyst-driven invocation.
 *
 *  PySpark sends Python ints as `LongType`. We expose Int overloads (for
 *  Scala/SQL literal callers) and Long overloads (for PySpark notebook
 *  callers). Wave 3 (`Quadbin_PointAsCell`) found this gap the hard way.
 */
object RST_Rasterize extends WithExpressionInfo {

    def eval(
        geomWkb: Array[Byte], value: Double,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, srid: Int,
        conf: UTF8String
    ): InternalRow = doInvoke(geomWkb, value, xmin, ymin, xmax, ymax, widthPx, heightPx, srid, conf)

    /** Long-overload for PySpark callers - promotes Int args sent as Long. */
    def eval(
        geomWkb: Array[Byte], value: Double,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Long, heightPx: Long, srid: Long,
        conf: UTF8String
    ): InternalRow = doInvoke(geomWkb, value, xmin, ymin, xmax, ymax,
        widthPx.toInt, heightPx.toInt, srid.toInt, conf)

    private def doInvoke(
        geomWkb: Array[Byte], value: Double,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, srid: Int,
        conf: UTF8String
    ): InternalRow =
        Option(
          RST_ErrorHandler.safeEval(
            () => execute(geomWkb, value, xmin, ymin, xmax, ymax, widthPx, heightPx, srid, conf),
            null,
            BinaryType,
            conf
          )
        ).map(_.asInstanceOf[InternalRow]).orNull

    /** Pure compute path - extracted for direct unit-testing without Spark. */
    def execute(
        geomWkb: Array[Byte], value: Double,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, srid: Int,
        conf: UTF8String
    ): InternalRow = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        if (geomWkb == null) return null
        val (ogrDs, layer) = VectorRasterBridge.buildOgrLayer(Seq((geomWkb, value)), srid)
        val rasterDs: Dataset = VectorRasterBridge.buildEmptyRaster(
            xmin, ymin, xmax, ymax, widthPx, heightPx, srid)
        try {
            val bands = Array(1)
            val burnValues = Array(0.0) // ignored; ATTRIBUTE option overrides
            val options = new JVector[String]()
            options.add(s"ATTRIBUTE=${VectorRasterBridge.ValueFieldName}")
            gdal.RasterizeLayer(rasterDs, bands, layer, burnValues, options)
            rasterDs.FlushCache()
            val bytes = VectorRasterBridge.toGTiffBytes(rasterDs)
            val mtd = Map(
                "driver" -> "GTiff",
                "extension" -> "tif",
                "size" -> bytes.length.toString,
                "parentPath" -> "",
                "all_parents" -> ""
            )
            val mapData = SerializationUtil.toMapData[String, String](mtd)
            // v2 8-field tile: cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata
            InternalRow.fromSeq(Seq(0L, bytes, null, null, null, null, null, mapData))
        } finally {
            rasterDs.delete()
            ogrDs.delete()
        }
    }

    override def name: String = "gbx_rst_rasterize"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 9 => RST_Rasterize(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_rasterize expects 9 arguments " +
            s"(geom_wkb, value, xmin, ymin, xmax, ymax, width_px, height_px, srid); got $n"
        )
    }

}
