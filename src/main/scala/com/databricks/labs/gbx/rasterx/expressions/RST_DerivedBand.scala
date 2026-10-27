package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver, ValidMask}
import com.databricks.labs.gbx.rasterx.operations.PixelCombineRasters
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.gdal.gdalconst.gdalconstConstants.GMF_PER_DATASET

/** Expression that computes a new band by applying a Python UDF to existing band values (tile, pythonCode, funcName). */
case class RST_DerivedBand(
    tile: Expression,
    pythonFuncExpr: Expression,
    funcNameExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, pythonFuncExpr, funcNameExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_DerivedBand.name
    override def replacement: Expression = invoke(RST_DerivedBand)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_DerivedBand extends WithExpressionInfo {

    def eval(row: InternalRow, pyFunc: UTF8String, funcName: UTF8String, conf: UTF8String): InternalRow =
        eval(row, pyFunc, funcName, conf, BinaryType)

    def eval(row: InternalRow, pythonFunc: UTF8String, funcName: UTF8String, conf: UTF8String, rdt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(row, rdt)
              val (newDs, newMtd) = execute(Seq(ds), mtd, pythonFunc.toString, funcName.toString)
              RasterDriver.releaseDataset(ds)
              val res = RasterSerializationUtil.tileToRow((cell, newDs, newMtd), rdt, exprConf.hConf)
              RasterDriver.releaseDataset(newDs)
              res
          },
          row,
          rdt
        )

    def execute(dss: Seq[Dataset], mtd: Map[String, String], pythonFunc: String, funcName: String): (Dataset, Map[String, String]) = {
        val (result, resultMtd) = PixelCombineRasters.combine(dss.toArray, mtd, pythonFunc, funcName)
        // The derived band has no alpha band of its own: where the one raster it is derived from is transparent or
        // masked (the rule of both tiers, [[ValidMask]]) it is invalid, carried in its own mask band. A raster
        // with neither is written as before; the tiles of an aggregate are stacked as plain bands, with no mask.
        if (dss.size == 1 && (GDAL.hasAlpha(dss.head) || (1 to dss.head.GetRasterCount()).exists(i => GDAL.hasOwnMask(dss.head.GetRasterBand(i))))) {
            val (w, h) = (result.GetRasterXSize(), result.GetRasterYSize())
            val valid = new Array[Byte](w * h)
            new ValidMask(dss.head).read(0, 0, w, h, valid)
            var i = 0
            while (i < valid.length) { valid(i) = if (valid(i) != 0) 255.toByte else 0.toByte; i += 1 }
            result.CreateMaskBand(GMF_PER_DATASET)
            result.GetRasterBand(1).GetMaskBand().WriteRaster(0, 0, w, h, valid)
            result.FlushCache()
        }
        (result, resultMtd)
    }

    override def name: String = "gbx_rst_derivedband"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_DerivedBand(c(0), c(1), c(2))
}
