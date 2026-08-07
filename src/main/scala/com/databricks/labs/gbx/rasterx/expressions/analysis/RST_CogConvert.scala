package com.databricks.labs.gbx.rasterx.expressions.analysis

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operator.OperatorOptions
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, TranslateOptions, gdal}

import java.util.{Vector => JVector}

/**
  * Convert a raster tile to a Cloud Optimized GeoTIFF (COG) layout via
  * `gdal.Translate -of COG`.
  *
  * COG is a regular GeoTIFF whose tiles + overviews are arranged so HTTP range
  * reads can extract small regions or pyramid levels without downloading the
  * full file. Use it as the final step of a "compose, then publish" pipeline:
  * cheaper to serve from object storage than a classic GTiff and recognised by
  * every modern raster tool.
  *
  *   - `compression` (default `"ZSTD"`): pixel compression — one of
  *     `NONE`, `DEFLATE`, `LZW`, `ZSTD`, `LERC`, `JPEG`, `WEBP`. ZSTD is the
  *     recommended default, consistent with the ZSTD baseline for all COG outputs.
  *   - `blocksize` (default `512`): internal tile size in pixels (square).
  *   - `overview_resampling` (default `"AVERAGE"`): downsampling algorithm
  *     used when GDAL auto-generates the overview pyramid — same set as
  *     `rst_buildoverviews`.
  *
  * Output is GTiff bytes (COG is a GTiff variant); downstream readers see
  * `metadata.driver = "GTiff"` with the COG layout markers in the header.
  */
case class RST_CogConvert(
    tile: Expression,
    compressionExpr: Expression,
    blocksizeExpr: Expression,
    overviewResamplingExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        tile, compressionExpr, blocksizeExpr, overviewResamplingExpr, ExpressionConfigExpr()
    )
    // Pin types: compression String, blocksize Int, overview_resampling String.
    override def inputTypes: Seq[DataType] = Seq(
        tile.dataType, StringType, IntegerType, StringType, StringType
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_CogConvert.name
    override def replacement: Expression = invoke(RST_CogConvert)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3))

}

object RST_CogConvert extends WithExpressionInfo {

    def eval(
        row: InternalRow, compression: UTF8String, blocksize: Int,
        overviewResampling: UTF8String, conf: UTF8String
    ): InternalRow = runDispatch(row, compression, blocksize, overviewResampling, conf, BinaryType)
    def eval(
        row: InternalRow, compression: UTF8String, blocksize: Long,
        overviewResampling: UTF8String, conf: UTF8String
    ): InternalRow = runDispatch(row, compression, blocksize.toInt, overviewResampling, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, compression: UTF8String, blocksize: Int,
        overviewResampling: UTF8String, conf: UTF8String, dt: DataType
    ): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val (resDs, resMtd) = execute(
                  ds, options,
                  Option(compression).map(_.toString).getOrElse("ZSTD"),
                  blocksize,
                  Option(overviewResampling).map(_.toString).getOrElse("AVERAGE")
              )
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    /** Pure compute path — extracted for direct unit-testing without Spark.
      *
      * Runs `gdal.Translate -of COG` with ZSTD+predictor compression (routed through OperatorOptions
      * for standard codec handling) and COG-specific blocksize/overview settings. Returns result
      * Dataset + metadata. Caller releases the returned Dataset.
      */
    def execute(
        ds: Dataset, options: Map[String, String],
        compression: String, blocksize: Int, overviewResampling: String
    ): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_CogConvert.execute: source Dataset is null")
        require(blocksize > 0, s"gbx_rst_cog_convert: blocksize must be > 0; got $blocksize")
        require(compression != null && compression.nonEmpty,
            "gbx_rst_cog_convert: compression must be non-empty")
        require(overviewResampling != null && overviewResampling.nonEmpty,
            "gbx_rst_cog_convert: overview_resampling must be non-empty")

        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        // Use .tif extension — downstream tools recognise COG as a GTiff variant.
        val outPath = s"/vsimem/cog_$uuid.tif"

        // Route through OperatorOptions to get standard ZSTD+predictor codec handling.
        // Pass the COG-specific options (format=COG, blocksize, overview_resampling) alongside compression.
        val writeOpts = options ++ Map(
            "format" -> "COG",
            "compression" -> compression,
            "blocksize" -> blocksize.toString,
            "overview_resampling" -> overviewResampling
        )

        // Build command string and route through appendOptions for standard predictor/codec.
        val baseCommand = "gdal_translate"
        val commandWithOptions = OperatorOptions.appendOptions(baseCommand, writeOpts, ds)

        // Parse the decorated command back into a TranslateOptions vector.
        val opts = OperatorOptions.parseOptions(commandWithOptions)
        // Add the OVERVIEW_RESAMPLING if not already present (appendOptions may not emit it).
        if (!opts.contains(s"OVERVIEW_RESAMPLING=$overviewResampling")) {
            opts.add("-co"); opts.add(s"OVERVIEW_RESAMPLING=$overviewResampling")
        }

        val tOpts = new TranslateOptions(opts)
        val result =
            try {
                gdal.Translate(outPath, ds, tOpts)
            } finally {
                tOpts.delete()
            }
        val errMsg = gdal.GetLastErrorMsg()
        if (result == null) {
            throw new RuntimeException(
                s"gbx_rst_cog_convert: gdal.Translate(-of COG) failed: " +
                  (if (errMsg == null || errMsg.isEmpty) "<no error>" else errMsg)
            )
        }
        result.FlushCache()

        val metadata = Map(
            "path" -> outPath,
            // COG is a GTiff variant on disk — downstream serialization expects GTiff here.
            "driver" -> "GTiff",
            "extension" -> "tif",
            "last_command" -> s"gdal.Translate(-of COG -co COMPRESS=$compression -co BLOCKSIZE=$blocksize -co OVERVIEW_RESAMPLING=$overviewResampling)",
            "last_error" -> (if (errMsg == null) "" else errMsg),
            "all_parents" -> Option(ds.GetDescription()).getOrElse(""),
            "size" -> "-1",
            "format" -> "GTiff",
            "compression" -> compression,
            "layout" -> "COG",
            "isZipped" -> "false",
            "isSubset" -> "false"
        )
        (result, metadata)
    }

    override def name: String = "gbx_rst_cog_convert"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 1 => RST_CogConvert(c(0), Literal("ZSTD"), Literal(512), Literal("AVERAGE"))
        case 2 => RST_CogConvert(c(0), c(1), Literal(512), Literal("AVERAGE"))
        case 3 => RST_CogConvert(c(0), c(1), c(2), Literal("AVERAGE"))
        case 4 => RST_CogConvert(c(0), c(1), c(2), c(3))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_cog_convert takes 1 to 4 arguments (tile, [compression, [blocksize, [overview_resampling]]]); got $n"
        )
    }

}
