package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import com.databricks.labs.gbx.gridx.grid.GeomDilation
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, LongType}
import org.apache.spark.unsafe.types.UTF8String

/** Catalyst expression: geometry-aware k-loop (hollow ring) for a custom-grid cell set.
  *
  * Returns the set of cell IDs reached at EXACTLY k Chebyshev steps from the geometry's
  * covering set, controlled by a dilation mode.
  *
  * Arguments: geomExpr (BINARY WKB or STRING WKT), gridExpr (grid-spec STRUCT),
  *            resolutionExpr (INT or LONG), kExpr (INT or LONG), modeExpr (STRING, optional).
  *
  * Returns: ARRAY<BIGINT> of cell IDs.
  */
case class Custom_GeometryKLoop(
    geomExpr:       Expression,
    gridExpr:       Expression,
    resolutionExpr: Expression,
    kExpr:          Expression,
    modeExpr:       Expression,
    coverageExpr:   Expression
) extends Expression with CodegenFallback {

    override def children: Seq[Expression] = Seq(geomExpr, gridExpr, resolutionExpr, kExpr, modeExpr, coverageExpr)
    override def dataType: DataType        = ArrayType(LongType)
    override def nullable: Boolean         = true
    override def foldable: Boolean         = children.forall(_.foldable)

    override def eval(input: InternalRow): Any = {
        val geomVal = geomExpr.eval(input)
        if (geomVal == null) return null
        val gridVal = gridExpr.eval(input)
        if (gridVal == null) return null

        val sys  = Custom_GridSpec.systemFromRow(gridVal.asInstanceOf[InternalRow])
        val res  = Custom_GridSpec.asInt(resolutionExpr.eval(input), "resolution")
        val k    = Custom_GridSpec.asInt(kExpr.eval(input), "k")
        val mRaw = modeExpr.eval(input)
        val mode = if (mRaw == null) GeomDilation.DEFAULT_MODE else mRaw.asInstanceOf[UTF8String].toString
        val cvRaw = coverageExpr.eval(input)
        val coverage = if (cvRaw == null) GeomDilation.DEFAULT_COVERAGE else cvRaw.asInstanceOf[UTF8String].toString

        GridErrorHandler.safeEval[ArrayData](null) {
            val geom = Custom_PointAsCell.decodeGeom(geomVal)
            ArrayData.toArrayData(sys.geometryKLoop(geom, res, k, mode, coverage).toArray)
        }
    }

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5))

}

/** Companion: SQL name gbx_custom_geomkloop, 4-/5-/6-arg builder. */
object Custom_GeometryKLoop extends WithExpressionInfo {

    override def name: String = "gbx_custom_geomkloop"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 4 => new Custom_GeometryKLoop(c(0), c(1), c(2), c(3), Literal(GeomDilation.DEFAULT_MODE), Literal(GeomDilation.DEFAULT_COVERAGE))
        case 5 => new Custom_GeometryKLoop(c(0), c(1), c(2), c(3), c(4), Literal(GeomDilation.DEFAULT_COVERAGE))
        case 6 => new Custom_GeometryKLoop(c(0), c(1), c(2), c(3), c(4), c(5))
        case n => throw new IllegalArgumentException(
            s"gbx_custom_geomkloop requires 4, 5, or 6 arguments; got $n")
    }

}
