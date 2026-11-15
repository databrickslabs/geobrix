package com.databricks.labs.gbx.rasterx.expressions.constructor

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, V2Tile}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.gdal

/**
  * The raster for construction of a raster tile. This should be the first
  * expression in the expression tree for a raster tile.
  */
case class RST_FromContent(
    contentExpr: Expression,
    driverExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(contentExpr, driverExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(BinaryType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_FromContent.name
    override def replacement: Expression = invoke(RST_FromContent)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_FromContent extends WithExpressionInfo {

    /** A NULL content or driver is NULL. A driver no GDAL build has raises naming it (outside `safeEval`, which would
      * turn it into an error tile); bytes GDAL cannot open, or a format the bytes rule refuses, are the tile's error
      * tile with the reason. The driver is stored as GDAL spells it. */
    def eval(content: Array[Byte], driver: UTF8String, conf: UTF8String): InternalRow = {
        if (content == null || driver == null) return null
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val drv = requireDriver(driver.toString)
        val body: () => InternalRow = () => {
            if (RasterDriver.sourceDriverOf(content, Map("driver" -> drv)).isEmpty) {
                throw new IllegalArgumentException(
                  "gbx_rst_fromcontent: the content cannot be opened as a raster. Fix: pass the bytes of a whole " +
                  "raster file in a format GDAL reads, such as the content column of spark.read.format('binaryFile').")
            }
            val mtd = Map(
              "driver" -> drv,
              "extension" -> GDAL.getExtension(drv),
              "size" -> content.length.toString
            )
            val mapData = SerializationUtil.toMapData[String, String](mtd)
            V2Tile.row(raster = content, metadata = mapData)
        }
        // The configured error mode applies: crashExpressions fails the call (4-argument overload); otherwise a failure
        // is the tile's error tile, which carries the reason (3-argument overload).
        if (exprConf.crashExpressions) {
            RST_ErrorHandler.safeEval(body, null: InternalRow, BinaryType, conf).asInstanceOf[InternalRow]
        } else {
            RST_ErrorHandler.safeEval(body, null: InternalRow, BinaryType)
        }
    }

    /** GDAL's spelling of the driver `name` (any case); IllegalArgumentException naming it when this build has none, or
      * when the driver is vector-only (no `DCAP_RASTER`), such as 'GeoJSON' or 'ESRI Shapefile'. */
    def requireDriver(name: String): String = {
        val d = gdal.GetDriverByName(name)
        if (d == null || d.GetMetadataItem("DCAP_RASTER") == null) {
            throw new IllegalArgumentException(
              s"gbx_rst_fromcontent: driver '$name' is not a GDAL raster driver in this build. " +
              "Fix: use a GDAL raster driver short name such as 'GTiff', 'PNG' or 'netCDF'.")
        }
        d.getShortName
    }

    override def name: String = "gbx_rst_fromcontent"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_FromContent(c(0), c(1))

}
