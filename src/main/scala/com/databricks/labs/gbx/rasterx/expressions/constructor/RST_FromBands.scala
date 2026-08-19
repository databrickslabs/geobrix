package com.databricks.labs.gbx.rasterx.expressions.constructor

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.MergeBands
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** The expression for stacking and resampling input bands. */
case class RST_FromBands(
    bandsExpr: Expression
) extends InvokedExpression {

    /** Raster DataType from the bands array element struct. */
    private def rasterType = RST_ExpressionUtil.arrayOfTileRasterType(
        RST_FromBands.name, bandsExpr, aggHint = None
    )
    /** Element field count from the declared input array element struct (3 for v1, 9 for v2). */
    private lazy val elementFieldCountLit: Expression =
        Literal(RST_ExpressionUtil.arrayOfTileElementFieldCount(bandsExpr), IntegerType)
    override def children: Seq[Expression] = Seq(bandsExpr, ExpressionConfigExpr(), elementFieldCountLit)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(rasterType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_FromBands.name
    override def replacement: Expression = invoke(RST_FromBands)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_FromBands extends WithExpressionInfo {

    // Called by Spark reflection when children = [bandsExpr, ExpressionConfigExpr()]  (v1 legacy path).
    def eval(row: ArrayData, conf: UTF8String): InternalRow = eval(row, conf, 3)

    // Called by Spark reflection when children = [bandsExpr, ExpressionConfigExpr(), elementFieldCountLit].
    // elementFieldCount is derived from the declared input array element struct (3=v1, 9=v2).
    def eval(row: ArrayData, conf: UTF8String, elementFieldCount: Int): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val tiles = RasterSerializationUtil.arrayToTiles(row, BinaryType, elementFieldCount)
              val (ds, mtd) = execute(tiles)
              tiles.foreach(t => RasterDriver.releaseDataset(t._2))
              RasterSerializationUtil.tileToRow((tiles.head._1, ds, mtd), BinaryType, exprConf.hConf)
          },
          row,
          BinaryType,
          elementFieldCount
        )

    def execute(tiles: Seq[(Long, Dataset, Map[String, String])]): (Dataset, Map[String, String]) = {
        val rasters = tiles.map(_._2)
        val metadata = tiles.head._3
        MergeBands.merge(rasters, metadata, "bilinear")
    }

    override def name: String = "gbx_rst_frombands"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_FromBands(c(0))

}
