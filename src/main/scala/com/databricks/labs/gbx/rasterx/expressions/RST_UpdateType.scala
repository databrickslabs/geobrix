package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Expression that changes the raster pixel data type (e.g. Byte, Int16, Float32). Arguments: tile, newType. */
case class RST_UpdateType(
    tile: Expression,
    newType: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, newType, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_UpdateType.name
    override def replacement: Expression = invoke(RST_UpdateType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_UpdateType extends WithExpressionInfo {

    def eval(row: InternalRow, newType: UTF8String, conf: UTF8String): InternalRow = eval(row, newType, conf, BinaryType)

    /** The data types, as GDAL names them; the SQL function takes them in any case. */
    val dataTypes: Seq[String] = Seq("Byte", "Int8", "UInt16", "Int16", "UInt32", "Int32", "Float32", "Float64")

    /** The name in `dataTypes` that `newType` names, in any case; the failure names the value. */
    def requireDataType(newType: String): String =
        dataTypes.find(_.equalsIgnoreCase(newType)).getOrElse(
          throw new IllegalArgumentException(
            s"gbx_rst_updatetype: unknown data type '$newType'; known: ${dataTypes.mkString(", ")}"
          )
        )

    def eval(row: InternalRow, newType: UTF8String, conf: UTF8String, rdt: DataType): InternalRow = {
        // A wrong type fails the call (naming it); only the tile's own failures become its error tile.
        val gdalType = if (RasterSerializationUtil.checksArguments(row) && newType != null) requireDataType(newType.toString) else newType.toString
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(row, rdt)
              val result = execute(ds, mtd, gdalType)
              RasterDriver.releaseDataset(ds)
              val res = RasterSerializationUtil.tileToRow((cell, result._1, result._2), rdt, exprConf.hConf)
              RasterDriver.releaseDataset(result._1)
              res
          },
          row,
          rdt
        )
    }

    def execute(ds: Dataset, options: Map[String, String], newType: String): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val driver = ds.GetDriver()
        val extension = GDAL.getExtension(driver.getShortName)
        val resPath = s"/vsimem/$uuid.$extension"
        GDALTranslate.executeTranslate(
          resPath,
          ds,
          command = s"gdal_translate -ot $newType",
          options
        )
    }

    override def name: String = "gbx_rst_updatetype"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_UpdateType(c(0), c(1))
}
