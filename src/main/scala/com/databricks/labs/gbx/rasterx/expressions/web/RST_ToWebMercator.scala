package com.databricks.labs.gbx.rasterx.expressions.web

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Reproject a tile to EPSG:3857 (web mercator).
 *
 *  Thin wrapper around `gdal.Warp -t_srs EPSG:3857 -r <resampling>` via `RasterProject`.
 *  Most slippy-map workflows start here because rasters typically arrive in EPSG:4326 or
 *  a UTM zone — neither renders directly in tile servers. Use this as the first step of
 *  a `rst_to_webmercator → rst_xyzpyramid → ...` pipeline, or call `rst_tilexyz` directly
 *  on a non-3857 raster (it warps to 3857 internally per-tile, but doing it once up-front
 *  is cheaper when many tiles share the same source).
 *
 *  Default resampling is `"bilinear"`, which preserves continuous-band rasters (DEM, NDVI).
 *  Use `"near"` for categorical rasters (land cover, classification masks).
 */
case class RST_ToWebMercator(
    tile: Expression,
    resamplingExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, resamplingExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_ToWebMercator.name
    override def replacement: Expression = invoke(RST_ToWebMercator)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))
}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_ToWebMercator extends WithExpressionInfo {

    /** Allowed GDAL warp resampling algorithms — keep aligned with `gdalwarp -r` options. */
    private val AllowedResampling: Set[String] = Set(
        "near", "bilinear", "cubic", "cubicspline", "lanczos",
        "average", "mode", "max", "min", "med", "q1", "q3"
    )

    def eval(row: InternalRow, resampling: UTF8String, conf: UTF8String): InternalRow =
        doInvoke(row, resampling, conf, BinaryType)

    private def doInvoke(row: InternalRow, resampling: UTF8String, conf: UTF8String, dt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val resampleStr = if (resampling == null) "bilinear" else resampling.toString
              // scalastyle:off caselocale
              val resampleLower = resampleStr.toLowerCase
              // scalastyle:on caselocale
              require(
                AllowedResampling.contains(resampleLower),
                s"rst_to_webmercator: unsupported resampling '$resampleStr'; allowed: ${AllowedResampling.toSeq.sorted.mkString(", ")}"
              )
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val (resultDs, metadata) = execute(ds, options, resampleLower)
              RasterDriver.releaseDataset(ds)
              val res = RasterSerializationUtil.tileToRow((cell, resultDs, metadata), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resultDs)
              res
          },
          row,
          dt
        )

    /** Warp `ds` to EPSG:3857 using `resampling` (lowercased gdalwarp -r value). Caller releases the returned Dataset. */
    def execute(ds: Dataset, options: Map[String, String], resampling: String): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val driver = ds.GetDriver()
        val extension = GDAL.getExtension(driver.getShortName)
        val resultPath = s"/vsimem/raster_webmerc_$uuid.$extension"
        GDALWarp.executeWarp(
          resultPath,
          Array(ds),
          options,
          command = s"gdalwarp -t_srs EPSG:3857 -r $resampling"
        )
    }

    override def name: String = "gbx_rst_to_webmercator"

    /** Builder: 1-arg (default bilinear) or 2-arg (caller-supplied resampling). */
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => {
        c.length match {
            case 1 => RST_ToWebMercator(c.head, Literal("bilinear"))
            case 2 => RST_ToWebMercator(c(0), c(1))
            case n => throw new IllegalArgumentException(
                s"gbx_rst_to_webmercator takes 1 or 2 arguments (tile, [resampling]); got $n"
            )
        }
    }
}
