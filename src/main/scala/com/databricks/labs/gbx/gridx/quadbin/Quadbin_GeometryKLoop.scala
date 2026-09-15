package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import com.databricks.labs.gbx.gridx.grid.{GeomDilation, Quadbin}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

/** Expression that returns the geometry-aware k-loop (hollow ring) for a quadbin grid.
  * Arguments: geom, resolution, k[, mode[, coverage]]. mode (default boundary-out) and
  * coverage (default coveras) are optional trailing args. Returns ARRAY<BIGINT>. */
case class Quadbin_GeometryKLoop(
    geom: Expression,
    resolution: Expression,
    k: Expression,
    mode: Expression,
    coverage: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(geom, resolution, k, mode, coverage)
    override def dataType: DataType = ArrayType(LongType)
    override def nullable: Boolean = true
    override def prettyName: String = Quadbin_GeometryKLoop.name
    override def replacement: Expression = invoke(Quadbin_GeometryKLoop)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2), nc(3), nc(4))

}

/** Companion: SQL name gbx_quadbin_geomkloop, builder, and eval. */
object Quadbin_GeometryKLoop extends WithExpressionInfo {

    private def modeStr(m: UTF8String): String =
        if (m == null) GeomDilation.DEFAULT_MODE else m.toString

    private def coverageStr(cv: UTF8String): String =
        if (cv == null) GeomDilation.DEFAULT_COVERAGE else cv.toString

    def eval(geom: Array[Byte], res: Int, k: Int, mode: UTF8String, coverage: UTF8String): ArrayData =
        GridErrorHandler.safeEval[ArrayData](null) {
            val geometry = JTS.fromWKB(geom)
            ArrayData.toArrayData(execute(geometry, res, k, modeStr(mode), coverageStr(coverage)).toArray)
        }

    def eval(geom: UTF8String, res: Int, k: Int, mode: UTF8String, coverage: UTF8String): ArrayData =
        GridErrorHandler.safeEval[ArrayData](null) {
            val geometry = JTS.fromWKT(geom.toString)
            ArrayData.toArrayData(execute(geometry, res, k, modeStr(mode), coverageStr(coverage)).toArray)
        }

    def eval(geom: Array[Byte], res: Long, k: Int, mode: UTF8String, coverage: UTF8String): ArrayData =
        eval(geom, res.toInt, k, mode, coverage)

    def eval(geom: UTF8String, res: Long, k: Int, mode: UTF8String, coverage: UTF8String): ArrayData =
        eval(geom, res.toInt, k, mode, coverage)

    // mode-only (coverage defaulted) — retained for direct test invocations
    def eval(geom: Array[Byte], res: Int, k: Int, mode: UTF8String): ArrayData =
        eval(geom, res, k, mode, null: UTF8String)

    def eval(geom: UTF8String, res: Int, k: Int, mode: UTF8String): ArrayData =
        eval(geom, res, k, mode, null: UTF8String)

    def eval(geom: Array[Byte], res: Long, k: Int, mode: UTF8String): ArrayData =
        eval(geom, res.toInt, k, mode)

    def eval(geom: UTF8String, res: Long, k: Int, mode: UTF8String): ArrayData =
        eval(geom, res.toInt, k, mode)

    // 3-arg eval retained for direct test invocations
    def eval(geom: Array[Byte], res: Int, k: Int): ArrayData =
        eval(geom, res, k, null: UTF8String, null: UTF8String)

    def eval(geom: UTF8String, res: Int, k: Int): ArrayData =
        eval(geom, res, k, null: UTF8String, null: UTF8String)

    def eval(geom: Array[Byte], res: Long, k: Int): ArrayData =
        eval(geom, res.toInt, k, null: UTF8String, null: UTF8String)

    def eval(geom: UTF8String, res: Long, k: Int): ArrayData =
        eval(geom, res.toInt, k, null: UTF8String, null: UTF8String)

    def execute(geom: Geometry, res: Int, k: Int, mode: String): Set[Long] =
        Quadbin.geometryKLoop(geom, res, k, mode)

    def execute(geom: Geometry, res: Int, k: Int, mode: String, coverage: String): Set[Long] =
        Quadbin.geometryKLoop(geom, res, k, mode, coverage)

    override def name: String = "gbx_quadbin_geomkloop"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 3 => new Quadbin_GeometryKLoop(c(0), c(1), c(2), Literal(GeomDilation.DEFAULT_MODE), Literal(GeomDilation.DEFAULT_COVERAGE))
        case 4 => new Quadbin_GeometryKLoop(c(0), c(1), c(2), c(3), Literal(GeomDilation.DEFAULT_COVERAGE))
        case 5 => new Quadbin_GeometryKLoop(c(0), c(1), c(2), c(3), c(4))
    }

}
