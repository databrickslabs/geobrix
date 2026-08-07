package com.databricks.labs.gbx.rasterx.expressions.pixel

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/**
  * Build internal overviews (image pyramids) on a raster tile via
  * `Dataset.BuildOverviews(resampling, levels)`.
  *
  *   - `levels`: array of integer downsampling factors (e.g. `[2, 4, 8, 16]`)
  *     — each factor produces one overview level downsampled by that ratio.
  *   - `resampling` (default `"average"`): one of the gdaladdo overview
  *     resampling algorithms — `nearest`, `average`, `rms`, `gauss`, `cubic`,
  *     `cubicspline`, `lanczos`, `bilinear`, `mode`, `none`.
  *
  * Overviews are embedded into the output GTiff itself (no `.ovr` sidecar).
  * Use this before tile-server publishing or `gdal_translate -of COG` to
  * pre-compute the zoom pyramid.
  */
case class RST_BuildOverviews(
    tile: Expression,
    levelsExpr: Expression,
    resamplingExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        tile, levelsExpr, resamplingExpr, ExpressionConfigExpr()
    )
    // Pin levels as ARRAY<INT> and resampling as String so SQL literals coerce cleanly.
    override def inputTypes: Seq[DataType] = Seq(
        tile.dataType, ArrayType(IntegerType), StringType, StringType
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_BuildOverviews.name
    override def replacement: Expression = invoke(RST_BuildOverviews)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

object RST_BuildOverviews extends WithExpressionInfo {

    /** Allowed gdaladdo resampling algorithms — keep aligned with the GDAL docs. */
    private val AllowedResampling: Set[String] = Set(
        "nearest", "average", "rms", "gauss", "cubic", "cubicspline",
        "lanczos", "bilinear", "mode", "none"
    )

    def eval(row: InternalRow, levels: ArrayData, resampling: UTF8String, conf: UTF8String): InternalRow =
        runDispatch(row, levels, resampling, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, levels: ArrayData, resampling: UTF8String, conf: UTF8String, dt: DataType
    ): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val resamplingStr = if (resampling == null) "average" else resampling.toString
              val levelsArr =
                  if (levels == null) Array.empty[Int]
                  else levels.toIntArray()
              val (resDs, resMtd) = execute(ds, options, levelsArr, resamplingStr)
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    /** Pure compute path — extracted for direct unit-testing without Spark. */
    def execute(
        ds: Dataset, options: Map[String, String], levels: Array[Int], resampling: String
    ): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_BuildOverviews.execute: source Dataset is null")
        require(levels != null && levels.nonEmpty,
            "gbx_rst_buildoverviews: levels must be a non-empty integer array (e.g. array(2, 4, 8))")
        levels.foreach { l =>
            require(l >= 2, s"gbx_rst_buildoverviews: each level must be >= 2; got $l")
        }
        val resamplingStr = if (resampling == null || resampling.isEmpty) "average" else resampling
        // scalastyle:off caselocale
        val resamplingLower = resamplingStr.toLowerCase
        // scalastyle:on caselocale
        require(
            AllowedResampling.contains(resamplingLower),
            s"gbx_rst_buildoverviews: unsupported resampling '$resamplingStr'; " +
              s"allowed: ${AllowedResampling.toSeq.sorted.mkString(", ")}"
        )

        // Make a writable copy first; BuildOverviews mutates the dataset in place.
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val extension = GDAL.getExtension(ds.GetDriver.getShortName)
        val outPath = s"/vsimem/buildoverviews_$uuid.$extension"
        val (outDs, mtd) = GDALTranslate.executeTranslate(outPath, ds, "gdal_translate", options)

        val rc = outDs.BuildOverviews(resamplingLower, levels)
        if (rc != 0) {
            val errMsg = org.gdal.gdal.gdal.GetLastErrorMsg()
            throw new RuntimeException(
                s"gbx_rst_buildoverviews: Dataset.BuildOverviews failed (rc=$rc): " +
                  (if (errMsg == null || errMsg.isEmpty) "<no error>" else errMsg)
            )
        }
        outDs.FlushCache()
        (outDs, mtd)
    }

    override def name: String = "gbx_rst_buildoverviews"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => RST_BuildOverviews(c(0), c(1), Literal("average"))
        case 3 => RST_BuildOverviews(c(0), c(1), c(2))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_buildoverviews takes 2 or 3 arguments (tile, levels, [resampling]); got $n"
        )
    }

}
