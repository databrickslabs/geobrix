package com.databricks.labs.gbx.rasterx.expressions.web

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operator.{GDALTranslate, GDALWarp}
import com.databricks.labs.gbx.rasterx.operations.BandAccessors
import com.databricks.labs.gbx.rasterx.tile.TileMath
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants

import java.util.Locale
import scala.util.Try

/** Render a single web-mercator XYZ tile from a raster.
 *
 *  Returns BINARY bytes of the PNG / JPEG / WEBP tile at `(z, x, y)`. Per-tile primitive:
 *
 *    1. Compute the EPSG:3857 bbox of the tile via [[TileMath.tileBboxWebMerc]].
 *    2. `gdal.Warp` the source into a `size x size` raster covering exactly that bbox
 *       (`-te xmin ymin xmax ymax -t_srs EPSG:3857 -ts size size -r <resampling>`).
 *    3. `gdal.Translate -of <format>` to materialize PNG / JPEG / WEBP bytes.
 *    4. Read the bytes back from `/vsimem/`.
 *
 *  Out-of-extent tiles return a transparent PNG (alpha=0) of the requested size - NOT
 *  null. Slippy-map tile servers expect a 200-status non-zero body even outside source
 *  coverage; returning null would surface as a 404 in the publishing pipeline.
 *
 *  Defaults: `format = "PNG"`, `size = 256`, `resampling = "bilinear"`, `rescale = "auto"`.
 *
 *  The `rescale` parameter controls 8-bit display contrast:
 *    - `"auto"` (default): uint8 source -> pass through unchanged; non-8-bit -> rescale
 *      to Byte using whole-dataset per-band min/max (computed once per source, no seams).
 *    - `"none"`: full-dtype-range mapping (raw crush behavior).
 *    - `"min,max"` string (e.g. `"8000,12000"`): use exactly these bounds for all bands.
 *  Applies to ALL output formats (PNG / JPEG / WEBP): the rescale is performed in
 *  `toDisplayRGBA` on the RGB bands before encoding, so JPEG and WEBP get the same
 *  contrast mapping as PNG (the earlier PNG-only limitation is gone as of the RGBA-output
 *  convergence). The alpha band is never rescaled.
 */
case class RST_TileXYZ(
    tileExpr: Expression,
    zExpr: Expression,
    xExpr: Expression,
    yExpr: Expression,
    formatExpr: Expression,
    sizeExpr: Expression,
    resamplingExpr: Expression,
    rescaleExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] =
        Seq(tileExpr, zExpr, xExpr, yExpr, formatExpr, sizeExpr, resamplingExpr, rescaleExpr, ExpressionConfigExpr())
    override def dataType: DataType = BinaryType
    override def nullable: Boolean = true
    override def prettyName: String = RST_TileXYZ.name
    override def replacement: Expression = rstInvoke(RST_TileXYZ, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7))
}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_TileXYZ extends WithExpressionInfo {

    /** GDAL drivers that can act as XYZ tile output formats. */
    private val AllowedFormats: Set[String] = Set("PNG", "JPEG", "WEBP")

    /** Allowed GDAL warp resampling algorithms. */
    private val AllowedResampling: Set[String] = Set(
        "near", "bilinear", "cubic", "cubicspline", "lanczos",
        "average", "mode", "max", "min", "med", "q1", "q3"
    )

    // Spark sends Python ints as LongType - we accept both Int and Long overloads. Int
    // overloads are needed for SQL literal default args; Long overloads cover the
    // PySpark-from-notebook case (Wave 3 found this in Quadbin_PointAsCell).
    def evalBinary(row: InternalRow, z: Int, x: Int, y: Int, format: UTF8String, size: Int, resampling: UTF8String, rescale: UTF8String, conf: UTF8String): Array[Byte] =
        doInvoke(row, z, x, y, format, size, resampling, rescale, conf, BinaryType)
    def evalBinary(row: InternalRow, z: Long, x: Long, y: Long, format: UTF8String, size: Long, resampling: UTF8String, rescale: UTF8String, conf: UTF8String): Array[Byte] =
        doInvoke(row, z.toInt, x.toInt, y.toInt, format, size.toInt, resampling, rescale, conf, BinaryType)
    def evalPath(row: InternalRow, z: Int, x: Int, y: Int, format: UTF8String, size: Int, resampling: UTF8String, rescale: UTF8String, conf: UTF8String): Array[Byte] =
        doInvoke(row, z, x, y, format, size, resampling, rescale, conf, StringType)
    def evalPath(row: InternalRow, z: Long, x: Long, y: Long, format: UTF8String, size: Long, resampling: UTF8String, rescale: UTF8String, conf: UTF8String): Array[Byte] =
        doInvoke(row, z.toInt, x.toInt, y.toInt, format, size.toInt, resampling, rescale, conf, StringType)

    private def doInvoke(
        row: InternalRow,
        z: Int, x: Int, y: Int,
        format: UTF8String, size: Int, resampling: UTF8String,
        rescale: UTF8String, conf: UTF8String, dt: DataType
    ): Array[Byte] = {
        val safe: () => Array[Byte] = () => {
            val exprConf = ExpressionConfig.fromB64(conf.toString)
            RST_ExpressionUtil.init(exprConf)
            val fmtStr = if (format == null) "PNG" else format.toString
            val resampleStr = if (resampling == null) "bilinear" else resampling.toString
            val rescaleStr = if (rescale == null) "auto" else rescale.toString
            // scalastyle:off caselocale
            val fmt = fmtStr.toUpperCase
            val resampleLower = resampleStr.toLowerCase
            // scalastyle:on caselocale
            require(AllowedFormats.contains(fmt), s"rst_tilexyz: format must be one of ${AllowedFormats.mkString(", ")}; got '$fmtStr'")
            require(AllowedResampling.contains(resampleLower),
                s"rst_tilexyz: unsupported resampling '$resampleStr'; allowed: ${AllowedResampling.toSeq.sorted.mkString(", ")}")
            require(size > 0 && size <= 4096, s"rst_tilexyz: size must be in (0, 4096]; got $size")
            val (_, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
            try execute(ds, options, z, x, y, fmt, size, resampleLower, rescaleStr)
            finally RasterDriver.releaseDataset(ds)
        }
        // safeEval wraps Throwables -> null; for BinaryType callers want bytes, never null.
        // On hard failure we still want a transparent PNG, so wrap the safe-eval ourselves.
        val result = Try(safe()).toOption.flatMap(Option(_))
        result.getOrElse(transparentPng(size))
    }

    /** Resolve the user `rescale` arg + open source `ds` into a pre-formatted GDAL
     *  `-scale_<b> min max 0 255` string for the byte-output translate step, or `""` for
     *  pass-through (uint8 `"auto"`, or `"none"`). Mirrors the light tier `_resolve_in_range`.
     *
     *  - `"none"`         -> `""` (today's full-dtype-range behavior).
     *  - `"min,max"` pair -> that pair repeated for every band.
     *  - `"auto"`:
     *      * uint8 source  -> `""` (already display-ready; pass through unchanged).
     *      * non-uint8     -> per-band whole-dataset (min,max) via BandAccessors.getMinMax
     *        (ComputeRasterMinMax exact). A constant band (min==max) widened to (min, min+1).
     *
     *  NOTE: `ComputeRasterMinMax` does not honor NoData masks unless the band NoData flag is
     *  set, whereas the light tier's `ds.statistics(approx=False)` honors the mask. On NoData
     *  rasters, heavy's min/max may include masked pixel values and can diverge slightly from
     *  light -- a known limitation. Fixtures for the parity gate are NoData-free.
     *
     *  The resolved flags are applied (via `rescaleByteMap` in `toDisplayRGBA`) to the RGB
     *  bands of every output format -- PNG, JPEG, and WEBP alike.
     */
    private[web] def resolveScale(ds: Dataset, rescale: String): String = {
        val mode = if (rescale == null) "auto" else rescale.trim
        // scalastyle:off caselocale
        val modeLower = mode.toLowerCase
        // scalastyle:on caselocale
        val nbands = ds.GetRasterCount

        // Use repeated -scale (one per band in order) rather than -scale_<n> form;
        // both are equivalent per GDAL docs but the repeated form has wider Java binding support.
        def fmtBands(pairs: Seq[(Double, Double)]): String =
            pairs.map { case (lo, hi) => s"-scale $lo $hi 0 255" }.mkString(" ")

        if (modeLower == "none") {
            ""
        } else if (modeLower == "auto") {
            val firstDt = ds.GetRasterBand(1).GetRasterDataType
            if (firstDt == gdalconstConstants.GDT_Byte) {
                "" // uint8 pass-through
            } else {
                val pairs = (1 to nbands).map { b =>
                    val (lo, hi) = BandAccessors.getMinMax(ds.GetRasterBand(b))
                    if (!(lo < hi)) (lo, lo + 1.0) else (lo, hi)
                }
                fmtBands(pairs)
            }
        } else {
            // explicit "min,max" pair (e.g. "8000,12000"), repeated per band.
            val parts = mode.split(",").map(_.trim)
            require(parts.length == 2, s"rst_tilexyz: rescale must be 'auto', 'none', or 'min,max'; got '$rescale'")
            val lo = parts(0).toDouble
            val hi = parts(1).toDouble
            require(lo < hi, s"rst_tilexyz: rescale (min,max) must have min < max; got ($lo, $hi)")
            fmtBands(Seq.fill(nbands)((lo, hi)))
        }
    }

    /** Back-compat: callers that do not specify rescale get today's behavior ("none"). */
    def execute(
        ds: Dataset,
        options: Map[String, String],
        z: Int, x: Int, y: Int,
        format: String, size: Int, resampling: String
    ): Array[Byte] = execute(ds, options, z, x, y, format, size, resampling, "none")

    /** Render the tile by warping `ds` to the (z,x,y) bbox + translating to bytes.
     *  If the tile bbox does not overlap the source extent, return a transparent PNG.
     *  Stats are read from the original source `ds` (pre-warp) so all tiles share one
     *  mapping -- no seams.
     */
    def execute(
        ds: Dataset,
        options: Map[String, String],
        z: Int, x: Int, y: Int,
        format: String, size: Int, resampling: String, rescale: String
    ): Array[Byte] = {
        val scaleFlags = resolveScale(ds, rescale)
        executeWithScale(ds, options, z, x, y, format, size, resampling, scaleFlags)
    }

    /** Render with a PRE-RESOLVED scale string (RST_XYZPyramid resolves once and passes it
     *  here for every tile so all tiles share one mapping -- no seams, stats read once).
     */
    private[web] def executeWithScale(
        ds: Dataset,
        options: Map[String, String],
        z: Int, x: Int, y: Int,
        format: String, size: Int, resampling: String, scaleFlags: String
    ): Array[Byte] = {
        val (xmin, ymin, xmax, ymax) = TileMath.tileBboxWebMerc(z, x, y)
        if (!datasetIntersectsWebMercBbox(ds, xmin, ymin, xmax, ymax)) {
            return transparentPng(size)
        }
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val warpPath = s"/vsimem/tilexyz_warp_$uuid.tif"
        val (warpedDs, warpedOpts) = GDALWarp.executeWarp(
          warpPath,
          Array(ds),
          // Inject format=GTiff into the intermediate so OperatorOptions.appendOptions
          // does not stamp PNG-specific flags on the warp step (we translate to PNG below).
          options ++ Map("format" -> "GTiff"),
          // -dstalpha: GDAL appends a binary (0/255) alpha band derived from the source's
          // valid-data mask (255=valid, 0=outside-footprint or NoData). warpedDs has N+1 bands.
          command = s"gdalwarp -t_srs EPSG:3857 -te $xmin $ymin $xmax $ymax -ts $size $size -r $resampling -dstalpha"
        )
        try {
            val extension = format.toLowerCase(Locale.ROOT) match {
                case "png"  => "png"
                case "jpeg" => "jpg"
                case "webp" => "webp"
                case other  => throw new IllegalArgumentException(s"rst_tilexyz: unknown format $other")
            }
            // Composite into display RGB(A): apply rescale to RGB bands, attach binary alpha.
            // For WEBP: try RGBA first; if the encoder rejects alpha (build without libwebp-alpha
            // support) it returns empty/null -- re-encode as 3-band RGB (outcome-driven fallback).
            // PNG always requests alpha; JPEG never does.
            val translateOpts = warpedOpts ++ Map("format" -> format, "extension" -> extension)
            def encode(wantAlpha: Boolean): Array[Byte] = {
                val rgbaDs = toDisplayRGBA(warpedDs, format, scaleFlags, wantAlpha)
                try {
                    val translatePath = s"/vsimem/tilexyz_out_$uuid.$extension"
                    val (resDs, _) = GDALTranslate.executeTranslate(
                      translatePath, rgbaDs, command = "gdal_translate", translateOpts)
                    Try(resDs.FlushCache()); Try(resDs.delete())
                    val bytes = gdal.GetMemFileBuffer(translatePath)
                    gdal.Unlink(translatePath)
                    bytes
                } finally RasterDriver.releaseDataset(rgbaDs)
            }
            val bytes: Array[Byte] = format match {
                case "WEBP" =>
                    val rgba = encode(wantAlpha = true)
                    if (rgba == null || rgba.isEmpty) encode(wantAlpha = false) else rgba
                case "JPEG" => encode(wantAlpha = false)
                case _      => encode(wantAlpha = true)  // PNG
            }
            if (bytes == null || bytes.isEmpty) transparentPng(size) else bytes
        } finally {
            RasterDriver.releaseDataset(warpedDs)
        }
    }

    /** Parse per-band (lo, hi) rescale ranges from the `-scale lo hi 0 255` flag string
     *  produced by resolveScale. Returns a Seq with one (lo, hi) per source RGB band (in
     *  order). When scaleFlags is empty (uint8 pass-through / "none"), returns Seq.empty. */
    private def rescaleByteMap(scaleFlags: String): Seq[(Double, Double)] = {
        if (scaleFlags == null || scaleFlags.trim.isEmpty) return Seq.empty
        // Split on "-scale" prefix to get per-band segments; drop empty heads.
        val segments = scaleFlags.trim.split("-scale").map(_.trim).filter(_.nonEmpty)
        segments.map { seg =>
            val parts = seg.split("\\s+")
            // Each segment: "lo hi 0 255"
            (parts(0).toDouble, parts(1).toDouble)
        }
    }

    /** Build a display RGB(A) GDT_Byte MEM dataset from the warped tile, matching the
     *  rio-tiler band mapping. `warpedDs` has the source bands plus a trailing binary
     *  alpha band (from `-dstalpha`). Returns a GDT_Byte MEM dataset ready to encode.
     *
     *  Band mapping (rio-tiler parity), where N = source band count (warpedDs has N+1):
     *    N==1 -> grey replicated to R=G=B; N==2 -> band1 grey R=G=B, band2 as alpha;
     *    N==3 -> R,G,B; N==4 -> R,G,B + band4 alpha; N>=5 -> first 3 bands R,G,B.
     *  Alpha for PNG/WEBP: the source's own alpha band (N in {2,4}) if present, else
     *  the warp's trailing -dstalpha band. JPEG drops alpha (3-band RGB).
     *  The `wantAlpha` parameter is set by the caller: PNG=true, JPEG=false, WEBP is
     *  tried with true first and retried with false if the encoder rejects alpha.
     *  The rescale flags apply to RGB bands only (never alpha). */
    private[web] def toDisplayRGBA(
        warpedDs: Dataset, format: String, scaleFlags: String, wantAlpha: Boolean
    ): Dataset = {
        val total = warpedDs.GetRasterCount        // = sourceBands + 1 (trailing -dstalpha)
        val n = total - 1                           // source band count
        val w = warpedDs.GetRasterXSize; val h = warpedDs.GetRasterYSize
        val outBands = if (wantAlpha) 4 else 3
        // mem is a native GDAL resource: release it on throw so it never leaks.
        // On the normal return path the caller owns it and is responsible for release.
        val mem = gdal.GetDriverByName("MEM").Create("", w, h, outBands, gdalconstConstants.GDT_Byte)
        try {
            // Band indices that become R, G, B and the alpha source band index (1-based).
            val (rgbSrc, alphaSrc): (Seq[Int], Int) = n match {
                case 1 => (Seq(1, 1, 1), total)     // grey -> RGB; alpha = trailing -dstalpha band
                case 2 => (Seq(1, 1, 1), 2)         // band1 grey -> RGB; band2 IS alpha (rio-tiler)
                case 3 => (Seq(1, 2, 3), total)     // RGB; alpha = trailing -dstalpha band
                case 4 => (Seq(1, 2, 3), 4)         // RGB; band4 IS alpha
                case _ => (Seq(1, 2, 3), total)     // >=5: first 3 -> RGB; alpha = trailing band
            }

            // Per-band (lo, hi) rescale ranges (apply rescale here, not at the translate step,
            // so alpha is never rescaled). For uint8 sources or "none" mode, scaleFlags is empty.
            val scalePairs = rescaleByteMap(scaleFlags)

            // Read source band values as Float64 (handles uint16/float32 sources uniformly),
            // optionally apply the per-band linear map, clamp, and write as Byte.
            def copyBandRescaled(srcBandIdx: Int, dstBandIdx: Int, pairIdx: Int): Unit = {
                val srcBand = warpedDs.GetRasterBand(srcBandIdx)
                val dstBand = mem.GetRasterBand(dstBandIdx)
                if (scalePairs.isEmpty) {
                    // Pass-through: source is already Byte (uint8 auto or "none" with Byte source).
                    val buf = Array.ofDim[Byte](w * h)
                    srcBand.ReadRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, buf)
                    dstBand.WriteRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, buf)
                } else {
                    // Rescale: read as Float64, apply linear map, clamp, write as Byte.
                    val floatBuf = Array.ofDim[Double](w * h)
                    srcBand.ReadRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Float64, floatBuf)
                    val (lo, hi) = if (pairIdx < scalePairs.length) scalePairs(pairIdx)
                                   else scalePairs.last // fallback: reuse last band's range
                    val range = hi - lo
                    val byteBuf = floatBuf.map { v =>
                        val scaled = if (range <= 0.0) 0.0 else (v - lo) / range * 255.0
                        math.max(0, math.min(255, math.round(scaled).toInt)).toByte
                    }
                    dstBand.WriteRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, byteBuf)
                }
            }

            def copyBandVerbatim(srcBandIdx: Int, dstBandIdx: Int): Unit = {
                val buf = Array.ofDim[Byte](w * h)
                warpedDs.GetRasterBand(srcBandIdx).ReadRaster(
                  0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, buf)
                mem.GetRasterBand(dstBandIdx).WriteRaster(
                  0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, buf)
            }

            rgbSrc.zipWithIndex.foreach { case (srcIdx, i) =>
                copyBandRescaled(srcIdx, i + 1, srcIdx - 1)
            }
            mem.GetRasterBand(1).SetColorInterpretation(gdalconstConstants.GCI_RedBand)
            mem.GetRasterBand(2).SetColorInterpretation(gdalconstConstants.GCI_GreenBand)
            mem.GetRasterBand(3).SetColorInterpretation(gdalconstConstants.GCI_BlueBand)
            if (wantAlpha) {
                copyBandVerbatim(alphaSrc, 4)
                mem.GetRasterBand(4).SetColorInterpretation(gdalconstConstants.GCI_AlphaBand)
            }
            mem.SetGeoTransform(warpedDs.GetGeoTransform())
            Option(warpedDs.GetProjection()).foreach(mem.SetProjection)
            mem
        } catch {
            case t: Throwable =>
                Try(mem.delete())
                throw t
        }
    }

    /** Cheap intersection test against the dataset's web-mercator extent. We assume the
     *  source has been warped to EPSG:3857 OR has a known SRS; if neither, fall back to
     *  the WGS84 world-bbox transform (i.e. assume coverage).
     */
    private def datasetIntersectsWebMercBbox(
        ds: Dataset, xmin: Double, ymin: Double, xmax: Double, ymax: Double
    ): Boolean = Try {
        val gt = ds.GetGeoTransform()
        val w = ds.GetRasterXSize.toDouble
        val h = ds.GetRasterYSize.toDouble
        val srcXmin = math.min(gt(0), gt(0) + w * gt(1) + h * gt(2))
        val srcXmax = math.max(gt(0), gt(0) + w * gt(1) + h * gt(2))
        val srcYmax = math.max(gt(3), gt(3) + w * gt(4) + h * gt(5))
        val srcYmin = math.min(gt(3), gt(3) + w * gt(4) + h * gt(5))
        // If source is not in EPSG:3857, this comparison is approximate - but it's only
        // used to short-circuit when there's clearly no overlap (e.g. the user asked
        // for a tile half a world away). For ambiguous cases we just warp and let GDAL
        // produce an empty raster - the bytes check at the end catches that.
        val srs = ds.GetSpatialRef
        if (srs != null && srs.GetAuthorityCode(null) == "3857") {
            !(srcXmax < xmin || srcXmin > xmax || srcYmax < ymin || srcYmin > ymax)
        } else {
            // Source not in 3857 - be permissive (let GDAL try; empty output => transparent).
            true
        }
    }.getOrElse(true)

    /** Returns a minimal RGBA transparent PNG of `size x size`. */
    private def transparentPng(size: Int): Array[Byte] = {
        val drv = gdal.GetDriverByName("MEM")
        val src = drv.Create("", size, size, 4, org.gdal.gdalconst.gdalconstConstants.GDT_Byte)
        // All bands are already zero-initialized - alpha=0 => fully transparent.
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val outPath = s"/vsimem/tilexyz_empty_$uuid.png"
        val (resDs, _) = GDALTranslate.executeTranslate(
          outPath,
          src,
          command = "gdal_translate",
          Map("format" -> "PNG", "extension" -> "png")
        )
        Try(resDs.FlushCache())
        Try(resDs.delete())
        val bytes = gdal.GetMemFileBuffer(outPath)
        gdal.Unlink(outPath)
        Try(src.delete())
        bytes
    }

    override def name: String = "gbx_rst_tilexyz"

    /** Builder: 4 to 8 args (tile, z, x, y, [format, [size, [resampling, [rescale]]]]). */
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => {
        c.length match {
            case 4 => RST_TileXYZ(c(0), c(1), c(2), c(3), Literal("PNG"), Literal(256), Literal("bilinear"), Literal("auto"))
            case 5 => RST_TileXYZ(c(0), c(1), c(2), c(3), c(4), Literal(256), Literal("bilinear"), Literal("auto"))
            case 6 => RST_TileXYZ(c(0), c(1), c(2), c(3), c(4), c(5), Literal("bilinear"), Literal("auto"))
            case 7 => RST_TileXYZ(c(0), c(1), c(2), c(3), c(4), c(5), c(6), Literal("auto"))
            case 8 => RST_TileXYZ(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7))
            case n => throw new IllegalArgumentException(
                s"gbx_rst_tilexyz takes 4 to 8 arguments (tile, z, x, y, [format, [size, [resampling, [rescale]]]]); got $n"
            )
        }
    }
}
