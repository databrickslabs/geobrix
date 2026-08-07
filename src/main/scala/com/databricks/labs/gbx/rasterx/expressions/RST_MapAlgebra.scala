package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.MapAlgebra
import com.databricks.labs.gbx.rasterx.operator.{GDALCalc, GDALTranslate}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import java.nio.file.{Files, Paths}
import scala.util.Try

/** The expression for map algebra. */
case class RST_MapAlgebra(
    tiles: Expression,
    jsonSpecExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.arrayOfTileRasterType(
        RST_MapAlgebra.name, tiles, aggHint = None
    )
    /** Element field count from the declared input array element struct (3 for v1, 8 for v2). */
    private lazy val elementFieldCountLit: Expression =
        Literal(RST_ExpressionUtil.arrayOfTileElementFieldCount(tiles), IntegerType)
    override def children: Seq[Expression] = Seq(tiles, jsonSpecExpr, ExpressionConfigExpr(), elementFieldCountLit)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(rasterType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_MapAlgebra.name
    override def replacement: Expression = invoke(RST_MapAlgebra)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_MapAlgebra extends WithExpressionInfo {


    // Called by Spark reflection when children = [tile, jsonSpecExpr, ExpressionConfigExpr()]  (v1 legacy path).
    def eval(array: ArrayData, spec: UTF8String, conf: UTF8String): InternalRow =
        eval(array, spec, conf, 3)

    // Called by Spark reflection when children = [tile, jsonSpecExpr, ExpressionConfigExpr(), elementFieldCountLit].
    // elementFieldCount is derived from the declared input array element struct (3=v1, 8=v2).
    def eval(array: ArrayData, spec: UTF8String, conf: UTF8String, elementFieldCount: Int): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val dss = RasterSerializationUtil.arrayToTiles(array, BinaryType, elementFieldCount)
              // GDAL calc does not work with /vsimem/ files, so we need to copy them to a local path
              val dssCpy = dss.map { ds =>
                  val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
                  val extension = GDAL.getExtension(ds._2.GetDriver.getShortName)
                  val path = s"${NodeFilePathUtil.rootPath}/$uuid.$extension"
                  val (dsCpy, mtd) = GDALTranslate.executeTranslate(path, ds._2, "gdal_translate", ds._3)
                  RasterDriver.releaseDataset(ds._2)
                  (ds._1, dsCpy, mtd, path)
              }
              val (result, mtd) = execute(dssCpy.map(_._2), dss.head._3, spec.toString)
              val res = RasterSerializationUtil.tileToRow((dssCpy.head._1, result, mtd), BinaryType, exprConf.hConf)
              dssCpy.foreach(ds => RasterDriver.releaseDataset(ds._2))
              dssCpy.foreach(ds => Files.deleteIfExists(Paths.get(ds._4)))
              // result is computed via gdalcalc so it is not in /vsimem/, we need to delete it manually
              val resPath = result.GetDescription()
              RasterDriver.releaseDataset(result)
              Try(Files.deleteIfExists(Paths.get(resPath)))
              res
          },
          array,
          BinaryType,
          elementFieldCount
        )

    def execute(dss: Seq[Dataset], options: Map[String, String], spec: String): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val extension = GDAL.getExtension(dss.head.GetDriver.getShortName)
        val resultPath = s"${NodeFilePathUtil.rootPath}/map_algebra_$uuid.$extension" // s"/vsimem/map_algebra_$uuid.$extension"
        val command = MapAlgebra.parseSpec(spec, resultPath, dss)
        GDALCalc.executeCalc(command, resultPath, options, dss.head)
    }

    override def name: String = "gbx_rst_mapalgebra"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_MapAlgebra(c(0), c(1))
}
