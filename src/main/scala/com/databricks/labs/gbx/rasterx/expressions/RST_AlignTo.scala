package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/**
  * Warp `tile` onto `reference_tile`'s grid — matching its CRS, extent, and pixel size —
  * returning the aligned tile. Nearest-neighbour resampling (`-r near`) is used so the output
  * lands exactly on the reference grid without fabricating interpolated values.
  *
  * This is the explicit fix for the alignment precondition of the cross-raster combine family
  * (`gbx_rst_combine*`): align each tile to a common reference, then combine.
  */
case class RST_AlignTo(
    tile: Expression,
    referenceTile: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, referenceTile, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_AlignTo.name
    override def replacement: Expression = invoke(RST_AlignTo)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1))

}

/** Companion: SQL name, builder, and eval entry point. */
object RST_AlignTo extends WithExpressionInfo {

    def eval(tileRow: InternalRow, refRow: InternalRow, conf: UTF8String): InternalRow =
        // An error tile as the reference comes out as itself, so its reason survives (the tile's own is passed on by the
        // dispatch wrapper and by safeEval).
        if (refRow != null && RasterSerializationUtil.isErrorTile(refRow) && !RasterSerializationUtil.isErrorTile(tileRow)) refRow
        else RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(tileRow, BinaryType)
              val refDs = RasterSerializationUtil.rowToDS(refRow, BinaryType)
              try {
                  val (resDs, resMtd) = execute(ds, refDs, options)
                  val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), BinaryType, exprConf.hConf)
                  RasterDriver.releaseDataset(resDs)
                  out
              } finally {
                  RasterDriver.releaseDataset(ds)
                  RasterDriver.releaseDataset(refDs)
              }
          },
          tileRow,
          BinaryType
        )

    /** Warp `ds` onto `refDs`'s grid (CRS + extent + pixel dimensions) with nearest-neighbour
      * resampling. Caller must release the returned Dataset. */
    def execute(ds: Dataset, refDs: Dataset, options: Map[String, String]): (Dataset, Map[String, String]) = {
        require(ds != null, "gbx_rst_align_to: source tile Dataset is null")
        require(refDs != null, "gbx_rst_align_to: reference tile Dataset is null")
        val w = refDs.GetRasterXSize
        val h = refDs.GetRasterYSize
        require(w > 0 && h > 0, s"gbx_rst_align_to: reference tile has non-positive size ${w}x$h")

        val gt = Array.ofDim[Double](6)
        refDs.GetGeoTransform(gt)
        // Corners of the reference extent (general affine gt; handles rotation/skew defensively).
        val xs = Seq(gt(0), gt(0) + w * gt(1) + h * gt(2))
        val ys = Seq(gt(3), gt(3) + w * gt(4) + h * gt(5))
        val (minX, maxX) = (xs.min, xs.max)
        val (minY, maxY) = (ys.min, ys.max)

        val (srsToken, tmpPath) = targetSrsToken(refDs)
        val outPath = newVsimemPath(ds)
        val tSrs = srsToken.map(t => s"-t_srs $t ").getOrElse("")
        val command =
            s"gdalwarp ${tSrs}-te ${fmt(minX)} ${fmt(minY)} ${fmt(maxX)} ${fmt(maxY)} -ts $w $h -r near"
        try {
            GDALWarp.executeWarp(outPath, Array(ds), options, command)
        } finally {
            tmpPath.foreach(p => scala.util.Try(Files.deleteIfExists(p)))
        }
    }

    /** Format a coordinate without scientific notation (the command string is space-tokenised,
      * and `-te`/`-ts` values must be single bare tokens). */
    private def fmt(d: Double): String = java.math.BigDecimal.valueOf(d).toPlainString

    /**
      * Resolve the reference CRS into a `-t_srs` token. Prefer an `AUTHORITY:CODE` (e.g. `EPSG:27700`)
      * — a single space-free token safe for the command parser. Fall back to a temp `.wkt` file
      * (path passed to `-t_srs`, which `SetFromUserInput` reads) for authority-less CRS. A CRS-less
      * reference returns `None` (match extent/size only, keeping the source CRS).
      *
      * @return `(token, tmpFileToDelete)` — the caller must delete `tmpFileToDelete` after the warp.
      */
    private def targetSrsToken(refDs: Dataset): (Option[String], Option[Path]) = {
        val wkt = refDs.GetProjection()
        if (wkt == null || wkt.isEmpty) (None, None)
        else {
            val sr = new SpatialReference()
            sr.ImportFromWkt(wkt)
            scala.util.Try(sr.AutoIdentifyEPSG())
            val name = Option(sr.GetAuthorityName(null))
            val code = Option(sr.GetAuthorityCode(null))
            sr.delete()
            (name, code) match {
                case (Some(n), Some(c)) if n.nonEmpty && c.nonEmpty => (Some(s"$n:$c"), None)
                case _ =>
                    val tmp = Files.createTempFile("gbx_align_srs_", ".wkt")
                    Files.write(tmp, wkt.getBytes(StandardCharsets.UTF_8))
                    (Some(tmp.toString), Some(tmp))
            }
        }
    }

    /** Build a /vsimem path with the source driver's natural extension (mirrors RST_ResampleHelper). */
    private def newVsimemPath(ds: Dataset): String = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val ext = GDAL.getExtension(ds.GetDriver().getShortName)
        s"/vsimem/raster_alignto_$uuid.$ext"
    }

    override def name: String = "gbx_rst_align_to"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => RST_AlignTo(c(0), c(1))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_align_to takes 2 arguments (tile, reference_tile); got $n")
    }

}
