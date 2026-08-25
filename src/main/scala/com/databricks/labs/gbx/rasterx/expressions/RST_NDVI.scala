package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.NDVI
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import java.nio.file.{Files, Paths}
import scala.util.Try

/** The expression for computing NDVI index. */
case class RST_NDVI(
    tile: Expression,
    redIdx: Expression,
    nirIdx: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, redIdx, nirIdx, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_NDVI.name
    override def replacement: Expression = invoke(RST_NDVI)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_NDVI extends WithExpressionInfo {

    def eval(row: InternalRow, redIdx: Int, nirIdx: Int, conf: UTF8String): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(row, BinaryType)
              val extension = GDAL.getExtension(ds.GetDriver.getShortName)
              val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
              val cpyPath = s"${NodeFilePathUtil.rootPath}/ndvi_temp_$uuid.$extension"
              val (dsCpy, dsMtd) = GDALTranslate.executeTranslate(cpyPath, ds, "gdal_translate", mtd)
              val (resultDs, resMtd) = execute(dsCpy, redIdx, nirIdx, dsMtd)
              if (resultDs == null) {
                  throw new Error(
                      s"""
                         |NDVI computation failed.
                         |${org.gdal.gdal.gdal.GetLastErrorMsg()}
                         |$resMtd
                         |""".stripMargin)
              }
              val resPath = resultDs.GetDescription()
              RasterDriver.releaseDataset(ds)
              RasterDriver.releaseDataset(dsCpy)
              Try(Files.deleteIfExists(Paths.get(cpyPath))) // ndvi_temp file is not stored in /vsimem/ so we need to delete it
              val res = RasterSerializationUtil.tileToRow((cell, resultDs, resMtd), BinaryType, exprConf.hConf)
              Try(Files.deleteIfExists(Paths.get(resPath))) // resultDs is not stored in /vsimem/ so we need to delete it
              RasterDriver.releaseDataset(resultDs)
              res
          },
          row,
          BinaryType
        )

    def execute(ds: Dataset, redIdx: Int, nirIdx: Int, options: Map[String, String]): (Dataset, Map[String, String]) = {
        NDVI.compute(ds, options, redIdx, nirIdx)
    }

    override def name: String = "gbx_rst_ndvi"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_NDVI(c(0), c(1), c(2))

}
