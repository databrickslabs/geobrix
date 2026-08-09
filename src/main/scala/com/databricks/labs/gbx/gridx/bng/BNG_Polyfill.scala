package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

/** Expression that fills a geometry with BNG cells at the given resolution (array of cell IDs). Arguments: geom, resolution. */
case class BNG_Polyfill(
    geom: Expression,
    resolution: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(geom, resolution)
    override def dataType: DataType = ArrayType(StringType)
    override def nullable: Boolean = true
    override def prettyName: String = BNG_Polyfill.name
    override def replacement: Expression = invoke(BNG_Polyfill)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name gbx_bng_polyfill, builder, and eval. */
object BNG_Polyfill extends WithExpressionInfo {

    def eval(geom: UTF8String, resolution: UTF8String): ArrayData = {
        val res = BNG.resolutionMap(resolution.toString) // PARAMETER: raises on bad resolution
        GridErrorHandler.safeEval[ArrayData](null) {
            val geometry = JTS.fromWKT(geom.toString)
            ArrayData.toArrayData(execute(geometry, res).map(UTF8String.fromString).toArray)
        }
    }

    def eval(geom: UTF8String, resolution: Int): ArrayData =
        GridErrorHandler.safeEval[ArrayData](null) {
            val geometry = JTS.fromWKT(geom.toString)
            ArrayData.toArrayData(execute(geometry, resolution).map(UTF8String.fromString).toArray)
        }

    def eval(geom: Array[Byte], resolution: UTF8String): ArrayData = {
        val res = BNG.resolutionMap(resolution.toString) // PARAMETER: raises on bad resolution
        GridErrorHandler.safeEval[ArrayData](null) {
            val geometry = JTS.fromWKB(geom)
            ArrayData.toArrayData(execute(geometry, res).map(UTF8String.fromString).toArray)
        }
    }

    def eval(geom: Array[Byte], resolution: Int): ArrayData =
        GridErrorHandler.safeEval[ArrayData](null) {
            val geometry = JTS.fromWKB(geom)
            ArrayData.toArrayData(execute(geometry, resolution).map(UTF8String.fromString).toArray)
        }

    def execute(geom: Geometry, resolution: Int): Iterator[String] = {
        BNG.polyfill(geom, resolution)
            .map(BNG.format)
    }

    def execute(geom: Geometry, resolution: String): Iterator[String] = {
        val res = BNG.resolutionMap(resolution)
        BNG.polyfill(geom, res)
            .map(BNG.format)
    }

    override def name: String = "gbx_bng_polyfill"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_Polyfill(c(0), c(1))


}
