package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.{GDALCalc, GDALTranslate, GDALWarp}
import com.databricks.labs.gbx.rasterx.operations.MapAlgebra
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.util.Try

/**
  * Computes a Canopy Height Model (CHM) from a DSM (Digital Surface Model) and a DEM
  * (Digital Elevation Model): CHM = clamp(align(DSM → DEM grid) − DEM, min = 0).
  *
  * Output is a Float32 single-band tile with NoData −9999. NoData in either input AND
  * alignment-padding pixels (pixels not covered by the source DSM) propagate to −9999.
  */
case class RST_Chm(
    tile: Expression,
    referenceTile: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, referenceTile, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Chm.name
    override def replacement: Expression = invoke(RST_Chm)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1))

}

/** Companion: SQL name, builder, and eval entry point. */
object RST_Chm extends WithExpressionInfo {

    def eval(tileRow: InternalRow, refRow: InternalRow, conf: UTF8String): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, dsmDs, options) = RasterSerializationUtil.rowToTile(tileRow, BinaryType)
              val demDs = RasterSerializationUtil.rowToDS(refRow, BinaryType)
              try {
                  val (resDs, resMtd) = execute(dsmDs, demDs, options)
                  val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), BinaryType, exprConf.hConf)
                  // The result is written by gdal_calc to the local filesystem; delete it after serialisation.
                  val resPath = resDs.GetDescription()
                  RasterDriver.releaseDataset(resDs)
                  Try(Files.deleteIfExists(Paths.get(resPath)))
                  out
              } finally {
                  RasterDriver.releaseDataset(dsmDs)
                  RasterDriver.releaseDataset(demDs)
              }
          },
          tileRow,
          BinaryType
        )

    /** Compute CHM = clamp(align(dsmDs → demDs grid) − demDs, min = 0).
      *
      * Steps:
      *   1. Warp dsmDs onto demDs's grid (CRS, extent, pixel size) with nearest-neighbour
      *      resampling; uncovered/padding pixels and source-nodata pixels are set to −9999.
      *   2. Run gdal_calc `maximum(A − B, 0)` over the aligned DSM and the DEM to produce
      *      a Float32 CHM with NoData −9999; input NoData is propagated automatically.
      *
      * Caller must release the returned Dataset. */
    def execute(dsmDs: Dataset, demDs: Dataset, options: Map[String, String]): (Dataset, Map[String, String]) = {
        require(dsmDs != null, "gbx_rst_chm: DSM Dataset is null")
        require(demDs  != null, "gbx_rst_chm: DEM Dataset is null")
        val alignedDsm = alignWithNoData(dsmDs, demDs, options, noData = -9999.0)
        try {
            chmCalc(alignedDsm, demDs, options, noData = -9999.0)
        } finally {
            RasterDriver.releaseDataset(alignedDsm)
        }
    }

    /** Warp dsmDs onto demDs's grid (CRS + extent + pixel dimensions) with nearest-neighbour
      * resampling. Uncovered/padding pixels and any source-nodata pixels are set to `noData`.
      * Returns a Dataset in /vsimem; caller must release. */
    private def alignWithNoData(
        dsmDs: Dataset,
        demDs: Dataset,
        options: Map[String, String],
        noData: Double
    ): Dataset = {
        val w = demDs.GetRasterXSize
        val h = demDs.GetRasterYSize
        require(w > 0 && h > 0, s"gbx_rst_chm: DEM has non-positive size ${w}x$h")

        val gt = Array.ofDim[Double](6)
        demDs.GetGeoTransform(gt)
        // Corners of the reference extent (general affine; handles rotation/skew defensively).
        val xs = Seq(gt(0), gt(0) + w * gt(1) + h * gt(2))
        val ys = Seq(gt(3), gt(3) + w * gt(4) + h * gt(5))
        val (minX, maxX) = (xs.min, xs.max)
        val (minY, maxY) = (ys.min, ys.max)

        val (srsToken, tmpPath) = targetSrsToken(demDs)
        val outPath = newVsimemPath(dsmDs)
        val tSrs = srsToken.map(t => s"-t_srs $t ").getOrElse("")
        // -dstnodata: single unquoted value — the command string is space-tokenised downstream
        // (OperatorOptions.parseOptions), so a quote-wrapped value would survive as stray-quote
        // tokens and gdalwarp would reject them. Mirrors RST_InitNoData.execute.
        val command =
            s"gdalwarp ${tSrs}-te ${fmt(minX)} ${fmt(minY)} ${fmt(maxX)} ${fmt(maxY)} -ts $w $h -r near -dstnodata $noData"
        try {
            GDALWarp.executeWarp(outPath, Array(dsmDs), options, command)._1
        } finally {
            tmpPath.foreach(p => Try(Files.deleteIfExists(p)))
        }
    }

    /** Compute CHM = maximum(A − B, 0) via gdal_calc where A = aligned DSM, B = DEM.
      * Output is Float32 with NoData = `noData`; input NoData is propagated automatically.
      *
      * gdal_calc cannot read /vsimem/ files, so both inputs are first translated to the
      * local filesystem under NodeFilePathUtil.rootPath (mirrors RST_MapAlgebra.eval).
      * Caller must release the returned Dataset. */
    private def chmCalc(
        alignedDsm: Dataset,
        demDs: Dataset,
        options: Map[String, String],
        noData: Double
    ): (Dataset, Map[String, String]) = {
        // Copy aligned DSM to a local path.
        val extA = GDAL.getExtension(alignedDsm.GetDriver.getShortName)
        val pathA =
            s"${NodeFilePathUtil.rootPath}/${java.util.UUID.randomUUID().toString.replace("-", "_")}.$extA"
        val (dsmCpy, _) = GDALTranslate.executeTranslate(pathA, alignedDsm, "gdal_translate", options)

        // Copy DEM to a local path.
        val extB = GDAL.getExtension(demDs.GetDriver.getShortName)
        val pathB =
            s"${NodeFilePathUtil.rootPath}/${java.util.UUID.randomUUID().toString.replace("-", "_")}.$extB"
        val (demCpy, _) = GDALTranslate.executeTranslate(pathB, demDs, "gdal_translate", options)

        try {
            val extOut = GDAL.getExtension(dsmCpy.GetDriver.getShortName)
            val resultPath =
                s"${NodeFilePathUtil.rootPath}/chm_${java.util.UUID.randomUUID().toString.replace("-", "_")}.$extOut"
            // extra_options carries --type and --NoDataValue; MapAlgebra.parseSpec appends them verbatim.
            val spec =
                s"""{"A_index":0,"B_index":1,"calc":"maximum(A-B,0)","extra_options":"--type=Float32 --NoDataValue=$noData"}"""
            val command = MapAlgebra.parseSpec(spec, resultPath, Seq(dsmCpy, demCpy))
            GDALCalc.executeCalc(command, resultPath, options, dsmCpy)
        } finally {
            RasterDriver.releaseDataset(dsmCpy)
            RasterDriver.releaseDataset(demCpy)
            Try(Files.deleteIfExists(Paths.get(pathA)))
            Try(Files.deleteIfExists(Paths.get(pathB)))
        }
    }

    /** Format a coordinate without scientific notation (the command string is space-tokenised
      * and `-te`/`-ts` values must be single bare tokens). */
    private def fmt(d: Double): String = java.math.BigDecimal.valueOf(d).toPlainString

    /** Resolve the DEM CRS into a -t_srs token.  Prefers an AUTHORITY:CODE (e.g. EPSG:32630)
      * — a single space-free token safe for the command parser. Falls back to a temp .wkt file
      * for authority-less CRS. Returns (token, tempFileToDelete). Mirrors RST_AlignTo. */
    private def targetSrsToken(refDs: Dataset): (Option[String], Option[Path]) = {
        val wkt = refDs.GetProjection()
        if (wkt == null || wkt.isEmpty) (None, None)
        else {
            val sr = new SpatialReference()
            sr.ImportFromWkt(wkt)
            Try(sr.AutoIdentifyEPSG())
            val name = Option(sr.GetAuthorityName(null))
            val code = Option(sr.GetAuthorityCode(null))
            sr.delete()
            (name, code) match {
                case (Some(n), Some(c)) if n.nonEmpty && c.nonEmpty => (Some(s"$n:$c"), None)
                case _ =>
                    val tmp = Files.createTempFile("gbx_chm_srs_", ".wkt")
                    Files.write(tmp, wkt.getBytes(StandardCharsets.UTF_8))
                    (Some(tmp.toString), Some(tmp))
            }
        }
    }

    /** Build a /vsimem path using the source driver's natural extension. */
    private def newVsimemPath(ds: Dataset): String = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val ext = GDAL.getExtension(ds.GetDriver().getShortName)
        s"/vsimem/raster_chm_$uuid.$ext"
    }

    override def name: String = "gbx_rst_chm"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => RST_Chm(c(0), c(1))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_chm takes 2 arguments (dsm_tile, dem_tile); got $n")
    }

}
