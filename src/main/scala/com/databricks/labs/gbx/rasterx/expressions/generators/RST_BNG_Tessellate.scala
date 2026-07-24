package com.databricks.labs.gbx.rasterx.expressions.generators

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.RasterTessellate
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, Literal}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
  * Returns a set of new rasters which are the result of the BNG tessellation of the
  * input raster.
  */
case class RST_BNG_Tessellate(
    tileExpr: Expression,
    resolutionExpr: Expression,
    modeExpr: Expression,
    exprConfExpr: Expression = ExpressionConfigExpr()
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    /** Raster DataType from the tile expression. */
    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def position: Boolean = false
    override def inline: Boolean = false
    override def elementSchema: StructType = StructType(Array(StructField("tile", dataType)))
    override def children: Seq[Expression] = Seq(tileExpr, resolutionExpr, modeExpr, exprConfExpr)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3))

    override def eval(input: InternalRow): IterableOnce[InternalRow] =
        RST_ErrorHandler.safeEval(
          () => {
              val conf = exprConfExpr.eval(input).asInstanceOf[UTF8String]
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val rawTile = tileExpr.eval(input).asInstanceOf[InternalRow]
              // BNG resolution accepts Int index (±1..±6) or String key ("1km", "100m", etc.)
              // BNG.getResolution handles both Int and UTF8String/String via pattern matching.
              val resRaw = resolutionExpr.eval(input)
              val resolution = BNG.getResolution(resRaw match {
                  case u: UTF8String => u.toString
                  case other         => other
              })
              val mode = modeExpr.eval(input).asInstanceOf[UTF8String].toString
              require(
                RasterTessellate.Modes.contains(mode),
                s"gbx_rst_bng_tessellate mode must be one of ${RasterTessellate.Modes.mkString(", ")}; got '$mode'"
              )
              val (_, ds, mtd) = RasterSerializationUtil.rowToTile(rawTile, rasterType)
              val iter = RasterTessellate.tessellateBngIter(ds, mtd, resolution, mode)
              RST_ExpressionUtil.addCleanupListener(iter)
              iter
                  .map { case (newCell, resDs, resMtd) =>
                      // BNG cell IDs are Strings (e.g. "TQ38SW"); parse back to Long for the tile struct
                      // cellid field. The string form is preserved in RASTERX_CELL_ID metadata on the Dataset.
                      val tile = RasterSerializationUtil.tileToRow((BNG.parse(newCell), resDs, resMtd), rasterType, exprConf.hConf)
                      RasterDriver.releaseDataset(resDs)
                      InternalRow.fromSeq(Seq(tile)) // Row wrapping in generator
                  }

          },
          input,
          rasterType
        )

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_BNG_Tessellate extends WithExpressionInfo {

    override def name: String = "gbx_rst_bng_tessellate"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) =>
        c.length match {
            case 2 => RST_BNG_Tessellate(c(0), c(1), Literal("covering"))
            case 3 => RST_BNG_Tessellate(c(0), c(1), c(2))
            case n =>
                throw new IllegalArgumentException(
                  s"gbx_rst_bng_tessellate takes 2 or 3 arguments (tile, resolution, [mode]); got $n"
                )
        }

}
