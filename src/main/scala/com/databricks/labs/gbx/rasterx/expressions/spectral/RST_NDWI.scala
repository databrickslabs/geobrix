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
  * Normalized Difference Water Index (NDWI, McFeeters 1996).
  *
  * Formula: ``(Green - NIR) / (Green + NIR)``
  *
  * Used to highlight open water bodies and suppress soil/vegetation in
  * remote-sensing imagery; positive values are typically water, negative are
  * land. Output is a single-band Float32 GTiff matching the input extent.
  */
case class RST_NDWI(
    tile: Expression,
    greenIdxExpr: Expression,
    nirIdxExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        tile, greenIdxExpr, nirIdxExpr, ExpressionConfigExpr()
    )
    override def inputTypes: Seq[DataType] = Seq(
        tile.dataType, IntegerType, IntegerType, StringType
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_NDWI.name
    override def replacement: Expression = invoke(RST_NDWI)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

object RST_NDWI extends WithExpressionInfo {

    def eval(row: InternalRow, greenIdx: Int, nirIdx: Int, conf: UTF8String): InternalRow =
        runDispatch(row, greenIdx, nirIdx, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, greenIdx: Int, nirIdx: Int, conf: UTF8String, dt: DataType
    ): InternalRow =
        SpectralIndexSpec.runRasterCalc(row, conf, dt) { calcDs =>
            execute(calcDs, greenIdx, nirIdx)
        }

    /** Pure compute path - extracted for direct unit-testing without Spark. */
    def execute(ds: Dataset, greenIdx: Int, nirIdx: Int): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_NDWI.execute: source Dataset is null")
        // A=green, B=NIR. NDWI = (A - B) / (A + B)
        val calc = "(A-B)/(A+B)"
        val spec = SpectralIndexSpec.singleSourceSpec(
            calc,
            Seq(("A", greenIdx), ("B", nirIdx))
        )
        RST_MapAlgebra.execute(Seq(ds), Map.empty, spec)
    }

    override def name: String = "gbx_rst_ndwi"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 3 => RST_NDWI(c(0), c(1), c(2))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_ndwi takes 3 arguments (tile, green_idx, nir_idx); got $n"
        )
    }

}
