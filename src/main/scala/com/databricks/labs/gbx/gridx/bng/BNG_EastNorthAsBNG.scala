package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.kernels.Coerce
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Expression that converts (easting, northing) to a BNG cell ID string. Arguments: easting, northing, resolution. */
case class BNG_EastNorthAsBNG(
    easting: Expression,
    northing: Expression,
    resolution: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(easting, northing, resolution)
    // Coordinates of any numeric or string type reach `eval` as DOUBLE (a non-number then fails the cast, naming the
    // value); the resolution is an INT index or a STRING (a size name or the digits of an index).
    override def inputTypes: Seq[DataType] = Seq(DoubleType, DoubleType, if (resolution.dataType == StringType) StringType else IntegerType)
    override def dataType: DataType = StringType
    override def nullable: Boolean = true
    override def prettyName: String = BNG_EastNorthAsBNG.name
    override def replacement: Expression = invoke(BNG_EastNorthAsBNG)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Companion: SQL name gbx_bng_eastnorthasbng, builder, and eval. */
object BNG_EastNorthAsBNG extends WithExpressionInfo {

    // PARAMETER: a resolution BNG does not define raises (naming the value) on every level; it is never a wrong cell.
    def eval(easting: Double, northing: Double, resolution: Int): UTF8String = {
        val res = Coerce.bngResolution(Int.box(resolution), digitStrings = false)
        GridErrorHandler.safeEval[UTF8String](null)(UTF8String.fromString(executeInt(easting, northing, res)))
    }

    def eval(easting: Double, northing: Double, resolution: UTF8String): UTF8String = {
        val res = Coerce.bngResolution(resolution.toString, digitStrings = true) // a size name, or the digits of an index
        GridErrorHandler.safeEval[UTF8String](null)(UTF8String.fromString(executeInt(easting, northing, res)))
    }

    def executeString(easting: Double, northing: Double, resolution: String): String = {
        val res = BNG.resolutionMap(resolution)
        val cellID = BNG.pointToCellID(easting, northing, res)
        BNG.format(cellID)
    }

    def executeInt(easting: Double, northing: Double, resolution: Int): String = {
        val cellID = BNG.pointToCellID(easting, northing, resolution)
        BNG.format(cellID)
    }

    override def name: String = "gbx_bng_eastnorthasbng"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_EastNorthAsBNG(c(0), c(1), c(2))


}
