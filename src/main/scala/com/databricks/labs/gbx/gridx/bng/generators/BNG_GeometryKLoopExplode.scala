package com.databricks.labs.gbx.gridx.bng.generators

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import com.databricks.labs.gbx.gridx.grid.{BNG, GeomDilation}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Generator that explodes the k-loop geometry of a BNG cell (from geom at resolution) into one row per cell.
  * Arguments: geom, resolution, k[, mode]. mode is optional (default: boundary-out). */
case class BNG_GeometryKLoopExplode(
    geom: Expression,
    resolution: Expression,
    k: Expression,
    mode: Expression,
    coverage: Expression
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    override def position: Boolean = false
    override def inline: Boolean = false
    override def children: Seq[Expression] = Seq(geom, resolution, k, mode, coverage)

    // noinspection DuplicatedCode
    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
        val geometryRaw = geom.eval(input)
        val resolutionRaw = resolution.eval(input)
        val kRaw = k.eval(input)
        val modeRaw = mode.eval(input)
        val coverageRaw = coverage.eval(input)
        if (geometryRaw == null || resolutionRaw == null || kRaw == null) {
            Seq.empty
        } else {
            val resolutionVal = resolution.dataType match {
                case StringType  => BNG.resolutionMap(resolutionRaw.asInstanceOf[UTF8String].toString)
                case IntegerType => resolutionRaw.asInstanceOf[Int]
            }
            val kVal = kRaw.asInstanceOf[Int]
            val modeStr = if (modeRaw == null) GeomDilation.DEFAULT_MODE else modeRaw.asInstanceOf[UTF8String].toString
            val coverageStr = if (coverageRaw == null) GeomDilation.DEFAULT_COVERAGE else coverageRaw.asInstanceOf[UTF8String].toString

            GridErrorHandler.safeEval[IterableOnce[InternalRow]](Iterator.empty) {
                val geometryVal = geom.dataType match {
                    case StringType => JTS.fromWKT(geometryRaw.asInstanceOf[UTF8String].toString)
                    case BinaryType => JTS.fromWKB(geometryRaw.asInstanceOf[Array[Byte]])
                }
                BNG.geometryKLoop(geometryVal, resolutionVal, kVal, modeStr, coverageStr)
                    .map(c => InternalRow.fromSeq(Seq(UTF8String.fromString(BNG.format(c)))))
            }
        }
    }

    override def elementSchema: StructType = StructType(Seq(StructField("cellid", StringType)))

    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2), nc(3), nc(4))

}

/** Companion: SQL name gbx_bng_geomkloopexplode, builder. */
object BNG_GeometryKLoopExplode extends WithExpressionInfo {

    override def name: String = "gbx_bng_geomkloopexplode"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 3 => new BNG_GeometryKLoopExplode(c(0), c(1), c(2), Literal(GeomDilation.DEFAULT_MODE), Literal(GeomDilation.DEFAULT_COVERAGE))
        case 4 => new BNG_GeometryKLoopExplode(c(0), c(1), c(2), c(3), Literal(GeomDilation.DEFAULT_COVERAGE))
        case 5 => new BNG_GeometryKLoopExplode(c(0), c(1), c(2), c(3), c(4))
    }

}
