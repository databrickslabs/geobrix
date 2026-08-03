package com.databricks.labs.gbx.rasterx.expressions.spectral

import com.databricks.labs.gbx.expressions.{ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.expressions.RST_MapAlgebra
import com.databricks.labs.gbx.rasterx.util.RST_ExpressionUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/**
  * Soil-Adjusted Vegetation Index (SAVI).
  *
  * Formula: ``(NIR - Red) / (NIR + Red + L) * (1 + L)``
  *
  * ``L`` is the soil-brightness correction factor (default ``0.5``, which
  * trades off sensitivity to vegetation cover and soil background; ``L=0``
  * reduces to NDVI; ``L=1`` is appropriate for very low vegetation cover).
  *
  * Output is a single-band Float32 GTiff matching the input raster's extent.
  */
case class RST_SAVI(
    tileExpr: Expression,
    redIdxExpr: Expression,
    nirIdxExpr: Expression,
    lExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        tileExpr, redIdxExpr, nirIdxExpr, lExpr, ExpressionConfigExpr()
    )
    override def inputTypes: Seq[DataType] = Seq(
        tileExpr.dataType, IntegerType, IntegerType, DoubleType, StringType
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_SAVI.name
    override def replacement: Expression = invoke(RST_SAVI)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3))

}

object RST_SAVI extends WithExpressionInfo {

    def eval(row: InternalRow, redIdx: Int, nirIdx: Int, l: Double, conf: UTF8String): InternalRow =
        runDispatch(row, redIdx, nirIdx, l, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, redIdx: Int, nirIdx: Int, l: Double, conf: UTF8String, dt: DataType
    ): InternalRow =
        SpectralIndexSpec.runRasterCalc(row, conf, dt) { calcDs =>
            execute(calcDs, redIdx, nirIdx, l)
        }

    /** Pure compute path - extracted for direct unit-testing without Spark. */
    def execute(ds: Dataset, redIdx: Int, nirIdx: Int, l: Double): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_SAVI.execute: source Dataset is null")
        // A=red, B=NIR. SAVI = (B - A) / (B + A + L) * (1 + L)
        val calc = s"((B-A)/(B+A+$l))*(1+$l)"
        val spec = SpectralIndexSpec.singleSourceSpec(
            calc,
            Seq(("A", redIdx), ("B", nirIdx))
        )
        RST_MapAlgebra.execute(Seq(ds), Map.empty, spec)
    }

    override def name: String = "gbx_rst_savi"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 3 => RST_SAVI(c(0), c(1), c(2), Literal(0.5))
        case 4 => RST_SAVI(c(0), c(1), c(2), c(3))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_savi takes 3 or 4 arguments (tile, red_idx, nir_idx, [L]); got $n"
        )
    }

}
