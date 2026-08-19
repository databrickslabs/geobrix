package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.CombineAVG
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Expression for combining rasters using average of pixels. Case class holding tiles (array of tiles); used as the catalyst node when gbx_rst_combineavg(tiles) is invoked in SQL or DataFrame API. */
case class RST_CombineAvg(
    tiles: Expression
) extends InvokedExpression {

    /** Raster DataType from the tiles array element struct. */
    private def rasterType = RST_ExpressionUtil.arrayOfTileRasterType(
        RST_CombineAvg.name, tiles, aggHint = Some("gbx_rst_combineavg_agg")
    )
    /** Element field count from the declared input array element struct (3 for v1, 9 for v2). */
    private lazy val elementFieldCountLit: Expression =
        Literal(RST_ExpressionUtil.arrayOfTileElementFieldCount(tiles), IntegerType)
    override def children: Seq[Expression] = Seq(tiles, ExpressionConfigExpr(), elementFieldCountLit)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(rasterType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_CombineAvg.name
    override def replacement: Expression = invoke(RST_CombineAvg)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_CombineAvg extends WithExpressionInfo {

    // Called by Spark reflection when children = [tile, ExpressionConfigExpr()]  (v1 legacy path).
    def eval(row: ArrayData, conf: UTF8String): InternalRow = eval(row, conf, 3)

    // Called by Spark reflection when children = [tile, ExpressionConfigExpr(), elementFieldCountLit].
    // elementFieldCount is derived from the declared input array element struct (3=v1, 9=v2).
    def eval(array: ArrayData, conf: UTF8String, elementFieldCount: Int): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val tiles = RasterSerializationUtil.arrayToTiles(array, BinaryType, elementFieldCount)
              val (cellID, combinedRaster, mtd) = execute(tiles)
              tiles.foreach(t => RasterDriver.releaseDataset(t._2))
              val res = RasterSerializationUtil.tileToRow((cellID, combinedRaster, mtd), BinaryType, exprConf.hConf)
              RasterDriver.releaseDataset(combinedRaster)
              res
          },
          array,
          BinaryType,
          elementFieldCount
        )

    def execute(tiles: Seq[(Long, Dataset, Map[String, String])]): (Long, Dataset, Map[String, String]) = {
        val cellID = if (tiles.map(_._1).groupBy(identity).size == 1) tiles.head._1 else -1L
        val (combinedRaster, mtd) = CombineAVG.compute(tiles.map(_._2).toArray, tiles.head._3)
        (cellID, combinedRaster, mtd)
    }

    override def name: String = "gbx_rst_combineavg"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_CombineAvg(c(0))

}
