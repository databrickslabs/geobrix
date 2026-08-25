package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.MergeRasters
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns a raster that is a result of merging an array of rasters. */
case class RST_Merge(
    tiles: Expression
) extends InvokedExpression {

    /** Raster DataType from the tiles array element struct. */
    private def rasterType = RST_ExpressionUtil.arrayOfTileRasterType(
        RST_Merge.name, tiles, aggHint = Some("gbx_rst_merge_agg")
    )
    /** Element field count from the declared input array element struct (3 for v1, 9 for v2). */
    private lazy val elementFieldCountLit: Expression =
        Literal(RST_ExpressionUtil.arrayOfTileElementFieldCount(tiles), IntegerType)
    override def children: Seq[Expression] = Seq(tiles, ExpressionConfigExpr(), elementFieldCountLit)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(rasterType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Merge.name
    override def replacement: Expression = invoke(RST_Merge)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_Merge extends WithExpressionInfo {

    // Called by Spark reflection when children = [tile, ExpressionConfigExpr()]  (v1 legacy path).
    def eval(array: ArrayData, conf: UTF8String): InternalRow = eval(array, conf, 3)

    // Called by Spark reflection when children = [tile, ExpressionConfigExpr(), elementFieldCountLit].
    // elementFieldCount is derived from the declared input array element struct (3=v1, 9=v2).
    def eval(array: ArrayData, conf: UTF8String, elementFieldCount: Int): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val tiles = RasterSerializationUtil.arrayToTiles(array, BinaryType, elementFieldCount)
              val dss = tiles.map(_._2)
              val cell = tiles.head._1
              val (mergedDs, options) = execute(dss.toArray, tiles.head._3)
              dss.foreach(ds => RasterDriver.releaseDataset(ds))
              val res = RasterSerializationUtil.tileToRow((cell, mergedDs, options), BinaryType, exprConf.hConf)
              RasterDriver.releaseDataset(mergedDs)
              res
          },
          array,
          BinaryType,
          elementFieldCount
        )

    def execute(dss: Array[Dataset], options: Map[String, String]): (Dataset, Map[String, String]) = {
        MergeRasters.merge(dss, options)
    }

    override def name: String = "gbx_rst_merge"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Merge(c(0))

}
