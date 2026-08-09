package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

/** Expression that returns the k-ring geometry (polygon) for a BNG cell at resolution. Arguments: cellId, resolution, k. */
case class BNG_GeometryKRing(
    geom: Expression,
    resolution: Expression,
    k: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(geom, resolution, k)
    override def dataType: DataType = ArrayType(StringType)
    override def nullable: Boolean = true
    override def prettyName: String = BNG_GeometryKRing.name
    override def replacement: Expression = invoke(BNG_GeometryKRing)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Companion: SQL name gbx_bng_geometrykring, builder, and eval. */
object BNG_GeometryKRing extends WithExpressionInfo {

    def eval(geom: UTF8String, res: Int, k: Int): ArrayData =
        GridErrorHandler.safeEval[ArrayData](null) {
            val geometry = JTS.fromWKT(geom.toString)
            ArrayData.toArrayData(execute(geometry, res, k).map(UTF8String.fromString).toArray)
        }

    def eval(geom: Array[Byte], res: Int, k: Int): ArrayData =
        GridErrorHandler.safeEval[ArrayData](null) {
            val geometry = JTS.fromWKB(geom)
            ArrayData.toArrayData(execute(geometry, res, k).map(UTF8String.fromString).toArray)
        }

    def eval(geom: UTF8String, res: UTF8String, k: Int): ArrayData = eval(geom, BNG.getResolution(res), k) // PARAMETER: getResolution raises on bad res

    def eval(geom: Array[Byte], res: UTF8String, k: Int): ArrayData = eval(geom, BNG.getResolution(res), k) // PARAMETER: getResolution raises on bad res

    def execute(geom: Geometry, res: Int, k: Int): Set[String] = {
        val kRing = BNG.geometryKRing(geom, res, k)
        kRing.map(BNG.format)
    }

    override def name: String = "gbx_bng_geomkring"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_GeometryKRing(c(0), c(1), c(2))


}
