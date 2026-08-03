package com.databricks.labs.gbx.rasterx.expressions.pixel

import com.databricks.labs.gbx.expressions.{ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.expressions.RST_MapAlgebra
import com.databricks.labs.gbx.rasterx.expressions.spectral.SpectralIndexSpec
import com.databricks.labs.gbx.rasterx.util.RST_ExpressionUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/**
  * Binarise a raster: every pixel matching the predicate `value <op> value` is
  * set to 1, every other valid pixel to 0. Output is a single-band Float32
  * GTiff sized to the input extent.
  *
  *   - `op`: one of ``">"``, ``">="``, ``"<"``, ``"<="``, ``"=="``, ``"!="``.
  *   - `value`: threshold value (Double).
  *
  * Built on `gbx_rst_mapalgebra` — gdal_calc receives a per-pixel formula
  * ``(A > value)*1`` (cast back to Float32 via ``--type=Float32``). NoData
  * cells stay NoData; the calc only fires over valid pixels.
  */
case class RST_Threshold(
    tileExpr: Expression,
    opExpr: Expression,
    valueExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        tileExpr, opExpr, valueExpr, ExpressionConfigExpr()
    )
    // Pin `value` as DoubleType so SQL decimal literals (e.g. ``5.0``) coerce cleanly.
    override def inputTypes: Seq[DataType] = Seq(
        tileExpr.dataType, StringType, DoubleType, StringType
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Threshold.name
    override def replacement: Expression = invoke(RST_Threshold)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

object RST_Threshold extends WithExpressionInfo {

    /** Supported comparison operators and their numpy equivalents. */
    private val AllowedOps: Set[String] = Set(">", ">=", "<", "<=", "==", "!=")

    def eval(row: InternalRow, op: UTF8String, value: Double, conf: UTF8String): InternalRow =
        runDispatch(row, op, value, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, op: UTF8String, value: Double, conf: UTF8String, dt: DataType
    ): InternalRow = {
        val opStr = if (op == null) null else op.toString
        SpectralIndexSpec.runRasterCalc(row, conf, dt) { calcDs =>
            execute(calcDs, opStr, value)
        }
    }

    /** Pure compute path — extracted for direct unit-testing without Spark. */
    def execute(ds: Dataset, op: String, value: Double): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_Threshold.execute: source Dataset is null")
        require(op != null && op.nonEmpty, "gbx_rst_threshold: op required (one of >, >=, <, <=, ==, !=)")
        require(!value.isNaN && !value.isInfinity,
            s"gbx_rst_threshold: value must be a finite Double; got $value")
        require(
            AllowedOps.contains(op),
            s"gbx_rst_threshold: unsupported op '$op'; allowed: ${AllowedOps.toSeq.sorted.mkString(", ")}"
        )
        // gdal_calc accepts numpy expressions — (A > value)*1 binarises.
        // Format the literal with %s so integer-valued doubles still parse.
        val calc = s"(A$op$value)*1"
        val spec = SpectralIndexSpec.singleSourceSpec(calc, Seq("A" -> 1))
        RST_MapAlgebra.execute(Seq(ds), Map.empty, spec)
    }

    override def name: String = "gbx_rst_threshold"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 3 => RST_Threshold(c(0), c(1), c(2))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_threshold takes 3 arguments (tile, op, value); got $n"
        )
    }

}
