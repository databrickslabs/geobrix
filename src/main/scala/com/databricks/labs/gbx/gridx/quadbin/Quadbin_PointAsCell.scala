package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.Quadbin
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

/** Expression that returns the quadbin cell (BIGINT) containing the (longitude, latitude) at the given resolution.
  * Arguments: longitude (double), latitude (double), resolution (int). Resolution range: 0..26. */
case class Quadbin_PointAsCell(
    longitude: Expression,
    latitude: Expression,
    resolution: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(longitude, latitude, resolution)
    override def dataType: DataType = LongType
    override def nullable: Boolean = true
    override def prettyName: String = Quadbin_PointAsCell.name
    override def replacement: Expression = invoke(Quadbin_PointAsCell)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Companion: SQL name gbx_quadbin_pointascell, builder, and eval entry. */
object Quadbin_PointAsCell extends WithExpressionInfo {

    def execute(longitude: Double, latitude: Double, resolution: Int): Long = Quadbin.pointToCell(longitude, latitude, resolution)

    def eval(longitude: Double, latitude: Double, resolution: Int): Long = execute(longitude, latitude, resolution)
    def eval(longitude: Double, latitude: Double, resolution: Long): Long = execute(longitude, latitude, resolution.toInt)

    override def name: String = "gbx_quadbin_pointascell"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new Quadbin_PointAsCell(c(0), c(1), c(2))
}
