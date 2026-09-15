package com.databricks.labs.gbx.gridx.custom.generators

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.custom.{Custom_GridSpec, Custom_PointAsCell}
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import com.databricks.labs.gbx.gridx.grid.GeomDilation
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Generator: explodes the geometry-aware k-loop (hollow ring) for a custom grid into one row per cell.
  *
  * Arguments: geom (BINARY or STRING), grid (grid-spec STRUCT), resolution (INT or LONG),
  *            k (INT or LONG), mode (STRING, optional — default boundary-out).
  * Yields rows with a single BIGINT cell id.
  */
case class Custom_GeometryKLoopExplode(
    geom:       Expression,
    grid:       Expression,
    resolution: Expression,
    k:          Expression,
    mode:       Expression,
    coverage:   Expression
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    override def position: Boolean = false
    override def inline:   Boolean = false
    override def children: Seq[Expression] = Seq(geom, grid, resolution, k, mode, coverage)

    // noinspection DuplicatedCode
    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
        val geomRaw = geom.eval(input)
        val gridRaw = grid.eval(input)
        val resRaw  = resolution.eval(input)
        val kRaw    = k.eval(input)
        if (geomRaw == null || gridRaw == null || resRaw == null || kRaw == null) {
            Seq.empty
        } else {
            val mRaw    = mode.eval(input)
            val modeStr = if (mRaw == null) GeomDilation.DEFAULT_MODE else mRaw.asInstanceOf[UTF8String].toString
            val cvRaw   = coverage.eval(input)
            val coverageStr = if (cvRaw == null) GeomDilation.DEFAULT_COVERAGE else cvRaw.asInstanceOf[UTF8String].toString
            val sys     = Custom_GridSpec.systemFromRow(gridRaw.asInstanceOf[InternalRow])
            val res     = Custom_GridSpec.asInt(resRaw, "resolution")
            val kVal    = Custom_GridSpec.asInt(kRaw, "k")
            GridErrorHandler.safeEval[IterableOnce[InternalRow]](Iterator.empty) {
                val geomDecoded = Custom_PointAsCell.decodeGeom(geomRaw)
                sys.geometryKLoop(geomDecoded, res, kVal, modeStr, coverageStr)
                    .map(cellId => InternalRow.fromSeq(Seq(cellId)))
            }
        }
    }

    override def elementSchema: StructType = StructType(Seq(StructField("cellid", LongType)))

    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5))

}

/** Companion: SQL name gbx_custom_geomkloopexplode, 4-/5-/6-arg builder. */
object Custom_GeometryKLoopExplode extends WithExpressionInfo {

    override def name: String = "gbx_custom_geomkloopexplode"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 4 => new Custom_GeometryKLoopExplode(c(0), c(1), c(2), c(3), Literal(GeomDilation.DEFAULT_MODE), Literal(GeomDilation.DEFAULT_COVERAGE))
        case 5 => new Custom_GeometryKLoopExplode(c(0), c(1), c(2), c(3), c(4), Literal(GeomDilation.DEFAULT_COVERAGE))
        case 6 => new Custom_GeometryKLoopExplode(c(0), c(1), c(2), c(3), c(4), c(5))
        case n => throw new IllegalArgumentException(
            s"gbx_custom_geomkloopexplode requires 4, 5, or 6 arguments; got $n")
    }

}
