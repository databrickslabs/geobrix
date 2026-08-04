package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.SpatialRefOps
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference

/**
  * Returns the raster's CRS as a canonical STRING: an authority string
  * (`EPSG:4326`, `ESRI:54008`, ...) when the CRS carries one, else the full WKT.
  *
  * This is the string companion to `gbx_rst_srid` (which returns the EPSG int, or 0
  * for a non-EPSG CRS such as ESRI World Sinusoidal). Use `rst_crs` to recover a
  * non-EPSG CRS that `rst_srid` cannot represent.
  */
case class RST_Crs(
    tileExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = StringType
    override def nullable: Boolean = true
    override def prettyName: String = RST_Crs.name
    override def replacement: Expression = invoke(RST_Crs)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_Crs extends WithExpressionInfo {

    def eval(row: InternalRow, conf: UTF8String): UTF8String = eval(row, conf, BinaryType)

    private def eval(row: InternalRow, conf: UTF8String, dt: DataType): UTF8String =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, dt)
                val res = execute(ds)
                RasterDriver.releaseDataset(ds)
                if (res == null) null else UTF8String.fromString(res)
            },
            row,
            dt,
            conf
          )
        ).map(_.asInstanceOf[UTF8String]).orNull

    /** Canonical CRS string (authority else WKT) for the dataset's projection. */
    def execute(ds: Dataset): String = {
        val proj = ds.GetProjection()
        if (proj == null || proj.isEmpty) return null
        val sr = new SpatialReference(proj)
        val canonical = SpatialRefOps.crsToCanonical(sr)
        sr.delete()
        canonical
    }

    override def name: String = "gbx_rst_crs"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Crs(c(0))

}
