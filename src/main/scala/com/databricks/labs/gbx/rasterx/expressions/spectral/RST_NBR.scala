package com.databricks.labs.gbx.rasterx.expressions.spectral

import com.databricks.labs.gbx.expressions.{ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.expressions.RST_MapAlgebra
import com.databricks.labs.gbx.rasterx.util.RST_ExpressionUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/**
  * Normalized Burn Ratio (NBR).
  *
  * Formula: ``(NIR - SWIR) / (NIR + SWIR)``
  *
  * Used to map burn severity from satellite imagery: high values (close to 1)
  * indicate healthy vegetation, low (or negative) values indicate burned
  * surfaces. The difference between pre-fire and post-fire NBR (``dNBR``) is
  * the canonical burn-severity index. Output is single-band Float32 GTiff.
  */
case class RST_NBR(
    tileExpr: Expression,
    nirIdxExpr: Expression,
    swirIdxExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        tileExpr, nirIdxExpr, swirIdxExpr, ExpressionConfigExpr()
    )
    override def inputTypes: Seq[DataType] = Seq(
        tileExpr.dataType, IntegerType, IntegerType, StringType
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_NBR.name
    override def replacement: Expression = invoke(RST_NBR)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

object RST_NBR extends WithExpressionInfo {

    def eval(row: InternalRow, nirIdx: Int, swirIdx: Int, conf: UTF8String): InternalRow =
        runDispatch(row, nirIdx, swirIdx, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, nirIdx: Int, swirIdx: Int, conf: UTF8String, dt: DataType
    ): InternalRow =
        SpectralIndexSpec.runRasterCalc(row, conf, dt) { calcDs =>
            execute(calcDs, nirIdx, swirIdx)
        }

    /** Pure compute path - extracted for direct unit-testing without Spark. */
    def execute(ds: Dataset, nirIdx: Int, swirIdx: Int): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_NBR.execute: source Dataset is null")
        // A=NIR, B=SWIR. NBR = (A - B) / (A + B)
        val calc = "(A-B)/(A+B)"
        val spec = SpectralIndexSpec.singleSourceSpec(
            calc,
            Seq(("A", nirIdx), ("B", swirIdx))
        )
        RST_MapAlgebra.execute(Seq(ds), Map.empty, spec)
    }

    override def name: String = "gbx_rst_nbr"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 3 => RST_NBR(c(0), c(1), c(2))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_nbr takes 3 arguments (tile, nir_idx, swir_idx); got $n"
        )
    }

}
