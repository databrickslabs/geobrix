package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import com.databricks.labs.gbx.gridx.grid.{BNG, GeomDilation}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

/** Expression that returns the k-loop geometry (polygon) for a BNG cell at resolution.
  * Arguments: geom, resolution, k[, mode[, coverage]]. mode (default boundary-out) and
  * coverage (default coveras) are optional trailing args. */
case class BNG_GeometryKLoop(
    geom: Expression,
    resolution: Expression,
    k: Expression,
    mode: Expression,
    coverage: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(geom, resolution, k, mode, coverage)
    override def dataType: DataType = ArrayType(StringType)
    override def nullable: Boolean = true
    override def prettyName: String = BNG_GeometryKLoop.name
    override def replacement: Expression = invoke(BNG_GeometryKLoop)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2), nc(3), nc(4))

}

/** Companion: SQL name gbx_bng_geomkloop, builder, and eval. */
object BNG_GeometryKLoop extends WithExpressionInfo {

    private def modeStr(m: UTF8String): String =
        if (m == null) GeomDilation.DEFAULT_MODE else m.toString

    private def coverageStr(cv: UTF8String): String =
        if (cv == null) GeomDilation.DEFAULT_COVERAGE else cv.toString

    def eval(geom: Array[Byte], res: Int, k: Int, mode: UTF8String, coverage: UTF8String): ArrayData =
        GridErrorHandler.safeEval[ArrayData](null) {
            val geometry = JTS.fromWKB(geom)
            ArrayData.toArrayData(execute(geometry, res, k, modeStr(mode), coverageStr(coverage)).map(UTF8String.fromString).toArray)
        }

    def eval(geom: UTF8String, res: Int, k: Int, mode: UTF8String, coverage: UTF8String): ArrayData =
        GridErrorHandler.safeEval[ArrayData](null) {
            val geometry = JTS.fromWKT(geom.toString)
            ArrayData.toArrayData(execute(geometry, res, k, modeStr(mode), coverageStr(coverage)).map(UTF8String.fromString).toArray)
        }

    def eval(geom: UTF8String, res: UTF8String, k: Int, mode: UTF8String, coverage: UTF8String): ArrayData =
        eval(geom, BNG.getResolution(res), k, mode, coverage)

    def eval(geom: Array[Byte], res: UTF8String, k: Int, mode: UTF8String, coverage: UTF8String): ArrayData =
        eval(geom, BNG.getResolution(res), k, mode, coverage)

    // mode-only (coverage defaulted) — retained for direct test invocations
    def eval(geom: Array[Byte], res: Int, k: Int, mode: UTF8String): ArrayData =
        eval(geom, res, k, mode, null: UTF8String)

    def eval(geom: UTF8String, res: Int, k: Int, mode: UTF8String): ArrayData =
        eval(geom, res, k, mode, null: UTF8String)

    // 3-arg eval retained for backward compatibility and direct test invocations
    def eval(geom: Array[Byte], res: Int, k: Int): ArrayData =
        eval(geom, res, k, null: UTF8String, null: UTF8String)

    def eval(geom: UTF8String, res: Int, k: Int): ArrayData =
        eval(geom, res, k, null: UTF8String, null: UTF8String)

    def eval(geom: UTF8String, res: UTF8String, k: Int): ArrayData =
        eval(geom, BNG.getResolution(res), k) // PARAMETER: getResolution raises on bad res

    def eval(geom: Array[Byte], res: UTF8String, k: Int): ArrayData =
        eval(geom, BNG.getResolution(res), k) // PARAMETER: getResolution raises on bad res

    def eval(geom: UTF8String, res: UTF8String, k: Int, mode: UTF8String): ArrayData =
        eval(geom, BNG.getResolution(res), k, mode)

    def eval(geom: Array[Byte], res: UTF8String, k: Int, mode: UTF8String): ArrayData =
        eval(geom, BNG.getResolution(res), k, mode)

    // 3-arg execute retained for backward compat
    def execute(geom: Geometry, res: Int, k: Int): Set[String] =
        execute(geom, res, k, GeomDilation.DEFAULT_MODE, GeomDilation.DEFAULT_COVERAGE)

    def execute(geom: Geometry, res: Int, k: Int, mode: String): Set[String] =
        execute(geom, res, k, mode, GeomDilation.DEFAULT_COVERAGE)

    def execute(geom: Geometry, res: Int, k: Int, mode: String, coverage: String): Set[String] = {
        val kLoop = BNG.geometryKLoop(geom, res, k, mode, coverage)
        kLoop.map(BNG.format)
    }

    override def name: String = "gbx_bng_geomkloop"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 3 => new BNG_GeometryKLoop(c(0), c(1), c(2), Literal(GeomDilation.DEFAULT_MODE), Literal(GeomDilation.DEFAULT_COVERAGE))
        case 4 => new BNG_GeometryKLoop(c(0), c(1), c(2), c(3), Literal(GeomDilation.DEFAULT_COVERAGE))
        case 5 => new BNG_GeometryKLoop(c(0), c(1), c(2), c(3), c(4))
    }

}
