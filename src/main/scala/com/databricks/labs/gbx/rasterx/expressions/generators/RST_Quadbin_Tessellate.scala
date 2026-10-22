package com.databricks.labs.gbx.rasterx.expressions.generators

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
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
  * Returns a set of new rasters which are the result of the quadbin tessellation of the
  * input raster.
  */
case class RST_Quadbin_Tessellate(
    tile: Expression,
    resolutionExpr: Expression,
    assignmentExpr: Expression,
    coverageExpr: Expression,
    exprConfExpr: Expression = ExpressionConfigExpr()
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    /** Raster DataType from the tile expression. */
    private def rasterType = RST_ExpressionUtil.rasterType(tile)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def position: Boolean = false
    override def inline: Boolean = false
    override def elementSchema: StructType = StructType(Array(StructField("tile", dataType)))
    override def children: Seq[Expression] = Seq(tile, resolutionExpr, assignmentExpr, coverageExpr, exprConfExpr)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4))

    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
        // The tile row is read once, outside the guarded body, so a failure's error tile can carry its cellid.
        val rawTile = tile.eval(input).asInstanceOf[InternalRow]
        RST_ErrorHandler.safeEval(
          () => {
              val conf = exprConfExpr.eval(input).asInstanceOf[UTF8String]
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val resolution = resolutionExpr.eval(input).asInstanceOf[Int]
              val assignment = assignmentExpr.eval(input).asInstanceOf[UTF8String].toString
              val coverage   = coverageExpr.eval(input).asInstanceOf[UTF8String].toString
              require(
                RasterTessellate.Assignments.contains(assignment),
                s"gbx_rst_quadbin_tessellate assignment must be one of ${RasterTessellate.Assignments.mkString(", ")}; got '$assignment'"
              )
              require(
                RasterTessellate.Coverages.contains(coverage),
                s"gbx_rst_quadbin_tessellate coverage must be one of ${RasterTessellate.Coverages.mkString(", ")}; got '$coverage'"
              )
              val (_, ds, mtd) = RasterSerializationUtil.rowToTile(rawTile, rasterType)
              val iter = RasterTessellate.tessellateQuadbinIter(ds, mtd, resolution, assignment, coverage)
              RST_ExpressionUtil.addCleanupListener(iter)
              iter
                  .map { case (newCell, resDs, resMtd) =>
                      val augMtd = resMtd + ("gridSystem" -> "quadbin")
                      val tile = RasterSerializationUtil.tileToRow((newCell, resDs, augMtd), rasterType, exprConf.hConf)
                      RasterDriver.releaseDataset(resDs)
                      InternalRow.fromSeq(Seq(tile)) // Row wrapping in generator
                  }

          },
          rawTile,
          rasterType
        )
    }

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_Quadbin_Tessellate extends WithExpressionInfo {

    override def name: String = "gbx_rst_quadbin_tessellate"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) =>
        c.length match {
            case 2 => RST_Quadbin_Tessellate(c(0), c(1), Literal("centroid"), Literal("complete"))
            case 3 => RST_Quadbin_Tessellate(c(0), c(1), c(2), Literal("complete"))
            case 4 => RST_Quadbin_Tessellate(c(0), c(1), c(2), c(3))
            case n =>
                throw new IllegalArgumentException(
                  s"gbx_rst_quadbin_tessellate takes 2 to 4 arguments (tile, resolution, [assignment], [coverage]); got $n"
                )
        }

}
