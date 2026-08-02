package com.databricks.labs.gbx.rasterx.expressions.spectral

import com.databricks.labs.gbx.expressions.{ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.expressions.RST_MapAlgebra
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import com.databricks.labs.gbx.rasterx.util.RST_ExpressionUtil

/**
  * Enhanced Vegetation Index (EVI).
  *
  * Formula: ``G * (NIR - Red) / (NIR + C1 * Red - C2 * Blue + L)``
  *
  * Args: red, NIR and blue band indices (1-based), plus four MODIS-canonical
  * coefficients with defaults ``L=1.0``, ``C1=6.0``, ``C2=7.5``, ``G=2.5``.
  *
  * Output is a single-band Float32 GTiff matching the input raster's extent.
  *
  * Implementation: builds a JSON ``RST_MapAlgebra`` spec with the red/nir/blue
  * bands wired to A/B/C and delegates to ``RST_MapAlgebra.execute``.
  */
case class RST_EVI(
    tileExpr: Expression,
    redIdxExpr: Expression,
    nirIdxExpr: Expression,
    blueIdxExpr: Expression,
    lExpr: Expression,
    c1Expr: Expression,
    c2Expr: Expression,
    gExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        tileExpr, redIdxExpr, nirIdxExpr, blueIdxExpr, lExpr, c1Expr, c2Expr, gExpr, ExpressionConfigExpr()
    )
    // Pin types so SQL decimal literals (e.g. ``1.0``) coerce to Double cleanly
    // and band-index literals coerce to Int.
    override def inputTypes: Seq[DataType] = Seq(
        tileExpr.dataType, IntegerType, IntegerType, IntegerType,
        DoubleType, DoubleType, DoubleType, DoubleType, StringType
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_EVI.name
    override def replacement: Expression = invoke(RST_EVI)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7))

}

object RST_EVI extends WithExpressionInfo {

    def eval(
        row: InternalRow, redIdx: Int, nirIdx: Int, blueIdx: Int,
        l: Double, c1: Double, c2: Double, g: Double, conf: UTF8String
    ): InternalRow = runDispatch(row, redIdx, nirIdx, blueIdx, l, c1, c2, g, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, redIdx: Int, nirIdx: Int, blueIdx: Int,
        l: Double, c1: Double, c2: Double, g: Double, conf: UTF8String, dt: DataType
    ): InternalRow =
        SpectralIndexSpec.runRasterCalc(row, conf, dt) { calcDs =>
            execute(calcDs, redIdx, nirIdx, blueIdx, l, c1, c2, g)
        }

    /** Pure compute path - extracted for direct unit-testing without Spark. */
    def execute(
        ds: Dataset, redIdx: Int, nirIdx: Int, blueIdx: Int,
        l: Double, c1: Double, c2: Double, g: Double
    ): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_EVI.execute: source Dataset is null")
        // A=red, B=NIR, C=blue. EVI = G * (B - A) / (B + C1*A - C2*C + L)
        val calc = s"$g*((B-A)/(B+$c1*A-$c2*C+$l))"
        val spec = SpectralIndexSpec.singleSourceSpec(
            calc,
            Seq(("A", redIdx), ("B", nirIdx), ("C", blueIdx))
        )
        RST_MapAlgebra.execute(Seq(ds), Map.empty, spec)
    }

    override def name: String = "gbx_rst_evi"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 4 => RST_EVI(c(0), c(1), c(2), c(3), Literal(1.0), Literal(6.0), Literal(7.5), Literal(2.5))
        case 5 => RST_EVI(c(0), c(1), c(2), c(3), c(4), Literal(6.0), Literal(7.5), Literal(2.5))
        case 6 => RST_EVI(c(0), c(1), c(2), c(3), c(4), c(5), Literal(7.5), Literal(2.5))
        case 7 => RST_EVI(c(0), c(1), c(2), c(3), c(4), c(5), c(6), Literal(2.5))
        case 8 => RST_EVI(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_evi takes 4 to 8 arguments (tile, red_idx, nir_idx, blue_idx, [L, [C1, [C2, [G]]]]); got $n"
        )
    }

}
