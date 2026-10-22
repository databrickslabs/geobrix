package com.databricks.labs.gbx.rasterx.expressions.generators

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.custom.Custom_GridSpec
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
  * Tessellates a raster into custom-grid cells.
  *
  * SQL arg order: `gbx_rst_custom_tessellate(tile, grid, resolution, [assignment], [coverage])` — the
  * `grid` struct (from `gbx_custom_grid(...)`) sits immediately after `tile`, mirroring the
  * `gbx_custom_*` grid-op arg convention, with the H3 optional `[assignment]`,`[coverage]` trailing.
  * Emits Long-keyed chips (custom cell ids). The raster is expected to be in the grid's native CRS.
  */
case class RST_Custom_Tessellate(
    tile: Expression,
    gridExpr: Expression,
    resolutionExpr: Expression,
    assignmentExpr: Expression,
    coverageExpr: Expression,
    exprConfExpr: Expression = ExpressionConfigExpr()
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    private def rasterType = RST_ExpressionUtil.rasterType(tile)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def position: Boolean = false
    override def inline: Boolean = false
    override def elementSchema: StructType = StructType(Array(StructField("tile", dataType)))
    override def children: Seq[Expression] =
        Seq(tile, gridExpr, resolutionExpr, assignmentExpr, coverageExpr, exprConfExpr)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5))

    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
        // The tile row is read once, outside the guarded body, so a failure's error tile can carry its cellid.
        val rawTile = tile.eval(input).asInstanceOf[InternalRow]
        RST_ErrorHandler.safeEval(
          () => {
              val conf = exprConfExpr.eval(input).asInstanceOf[UTF8String]
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val gridSys = Custom_GridSpec.systemFromRow(gridExpr.eval(input).asInstanceOf[InternalRow])
              val resolution = resolutionExpr.eval(input).asInstanceOf[Int]
              val assignment = assignmentExpr.eval(input).asInstanceOf[UTF8String].toString
              val coverage   = coverageExpr.eval(input).asInstanceOf[UTF8String].toString
              require(
                RasterTessellate.Assignments.contains(assignment),
                s"gbx_rst_custom_tessellate assignment must be one of ${RasterTessellate.Assignments.mkString(", ")}; got '$assignment'"
              )
              require(
                RasterTessellate.Coverages.contains(coverage),
                s"gbx_rst_custom_tessellate coverage must be one of ${RasterTessellate.Coverages.mkString(", ")}; got '$coverage'"
              )
              val (_, ds, mtd) = RasterSerializationUtil.rowToTile(rawTile, rasterType)
              val iter = RasterTessellate.tessellateCustomIter(gridSys, ds, mtd, resolution, assignment, coverage)
              RST_ExpressionUtil.addCleanupListener(iter)
              iter
                  .map { case (newCell, resDs, resMtd) =>
                      val augMtd = resMtd + ("gridSystem" -> "custom")
                      val tile = RasterSerializationUtil.tileToRow((newCell, resDs, augMtd), rasterType, exprConf.hConf)
                      RasterDriver.releaseDataset(resDs)
                      InternalRow.fromSeq(Seq(tile))
                  }
          },
          rawTile,
          rasterType
        )
    }

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_Custom_Tessellate extends WithExpressionInfo {

    override def name: String = "gbx_rst_custom_tessellate"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) =>
        c.length match {
            case 3 => RST_Custom_Tessellate(c(0), c(1), c(2), Literal("centroid"), Literal("complete"))
            case 4 => RST_Custom_Tessellate(c(0), c(1), c(2), c(3), Literal("complete"))
            case 5 => RST_Custom_Tessellate(c(0), c(1), c(2), c(3), c(4))
            case n =>
                throw new IllegalArgumentException(
                  s"gbx_rst_custom_tessellate takes 3 to 5 arguments (tile, grid, resolution, [assignment], [coverage]); got $n"
                )
        }

}
