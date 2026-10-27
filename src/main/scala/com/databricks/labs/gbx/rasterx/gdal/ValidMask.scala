package com.databricks.labs.gbx.rasterx.gdal

import org.gdal.gdal.{Band, Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants.CE_None

/**
  * THE validity rule of both tiers (the lightweight tier's `_nodata.valid_mask`), applied for every band
  * count. A pixel of `ds` is valid when ALL of these hold:
  *
  *  - its alpha value is above 0, when `ds` has an alpha band ([[GDAL.alphaBandIndex]]: the last band, when
  *    it is labelled alpha);
  *  - some data band (any band but the alpha band) is valid by its own mask band: its NoData, the dataset's
  *    own per-dataset mask band, or the mask GDAL derives from the alpha band. A raster that is only an
  *    alpha band has no data band to ask, and this clause holds.
  *
  * GDAL derives a mask from the alpha band only for 2- and 4-band rasters, drops it when a NoData is
  * declared over the alpha band, and gives the alpha band itself an all-valid mask, so the alpha values are
  * read here: a 3-band gray+x+alpha or a 6-band RGB+extra+alpha raster, and a raster that declares NoData,
  * mask as gdalwarp does.
  *
  * Reads are by row window and reuse their buffers, so a caller bounds the memory by the window it asks for
  * (one byte per pixel for each of two buffers, and eight bytes per pixel for the alpha values, read as
  * Double so that any alpha above 0 stays above 0). [[readBand]] is the same rule for ONE band, for the
  * reducers and statistics that judge each band alone.
  * GDAL resources: reads only (creates and releases no dataset).
  */
final class ValidMask(ds: Dataset) {

    private val xSize = ds.getRasterXSize
    private val ySize = ds.getRasterYSize
    private val alphaIndex: Option[Int] = GDAL.alphaBandIndex(ds)
    private val dataBands: Array[Int] = (1 to ds.getRasterCount).filterNot(alphaIndex.contains).toArray
    private val dataMasks: Array[Band] = dataBands.map(b => ds.GetRasterBand(b).GetMaskBand())
    private val alphaBand: Option[Band] = alphaIndex.map(ds.GetRasterBand)
    private var scratch: Array[Byte] = Array.emptyByteArray
    private var alpha: Array[Double] = Array.emptyDoubleArray

    private def check(rc: Int, what: String, yOff: Int, h: Int): Unit =
        if (rc != CE_None) {
            throw new IllegalStateException(s"reading $what, rows $yOff..${yOff + h - 1}, failed: ${gdal.GetLastErrorMsg()}")
        }

    /** Fills `out(0 until w * h)` for the window `(xOff, yOff, w, h)`, row-major: non-zero where the pixel is
      * valid, 0 where it is not. */
    def read(xOff: Int, yOff: Int, w: Int, h: Int, out: Array[Byte]): Unit = {
        val n = w * h
        require(out.length >= n, s"the output holds ${out.length} pixels, the window $n")
        if (dataMasks.isEmpty) java.util.Arrays.fill(out, 0, n, 1.toByte)
        else {
            if (scratch.length < n && dataMasks.length > 1) scratch = new Array[Byte](n)
            var i = 0
            while (i < dataMasks.length) {
                val what = s"the band ${dataBands(i)} mask"
                if (i == 0) check(dataMasks(i).ReadRaster(xOff, yOff, w, h, out), what, yOff, h)
                else {
                    check(dataMasks(i).ReadRaster(xOff, yOff, w, h, scratch), what, yOff, h)
                    var p = 0
                    while (p < n) { if (out(p) == 0) out(p) = scratch(p); p += 1 }
                }
                i += 1
            }
        }
        gateByAlpha(xOff, yOff, w, h, out)
    }

    /** Sets `out(p)` to 0 where the alpha value of pixel `p` of the window is not above 0 (0, negative and NaN
      * alike). Does nothing when `ds` has no alpha band. */
    private def gateByAlpha(xOff: Int, yOff: Int, w: Int, h: Int, out: Array[Byte]): Unit =
        alphaBand.foreach { band =>
            val n = w * h
            if (alpha.length < n) alpha = new Array[Double](n)
            check(band.ReadRaster(xOff, yOff, w, h, alpha), s"the alpha band ${alphaIndex.get}", yOff, h)
            var p = 0
            while (p < n) { if (!(alpha(p) > 0.0)) out(p) = 0; p += 1 }
        }

    /** The per-band form of [[read]]: fills `out(0 until w * h)` for the window `(xOff, yOff, w, h)` with
      * non-zero where the pixel is valid FOR BAND `band` (1-based): valid by the band's own mask (its NoData,
      * the dataset's own mask band) and, when `ds` has an alpha band, opaque. The alpha band itself is
      * judged by its own mask only, as GDAL does: its values over every pixel, which is how an opacity is
      * summarised. A pixel that is NoData in one band still counts for another. */
    def readBand(band: Int, xOff: Int, yOff: Int, w: Int, h: Int, out: Array[Byte]): Unit = {
        require(band >= 1 && band <= ds.getRasterCount, s"band $band is not one of the ${ds.getRasterCount} bands")
        require(out.length >= w * h, s"the output holds ${out.length} pixels, the window ${w * h}")
        check(ds.GetRasterBand(band).GetMaskBand().ReadRaster(xOff, yOff, w, h, out), s"the band $band mask", yOff, h)
        if (!alphaIndex.contains(band)) gateByAlpha(xOff, yOff, w, h, out)
    }

    /** True when some pixel of the dataset is valid, read in row strips of at most [[ValidMask.StripPixels]]
      * pixels and stopping at the first valid one. */
    def anyValid(): Boolean = {
        val rows = math.max(1, math.min(ySize, ValidMask.StripPixels / math.max(1, xSize)))
        val strip = new Array[Byte](xSize * rows)
        var y0 = 0
        while (y0 < ySize) {
            val h = math.min(rows, ySize - y0)
            read(0, y0, xSize, h, strip)
            var p = 0
            while (p < xSize * h) { if (strip(p) != 0) return true; p += 1 }
            y0 += h
        }
        false
    }
}

object ValidMask {

    /** Pixels per validity strip (1 M): each buffer of a strip stays about 1 MB whatever the raster size. The
      * lightweight tier's `_STRIP_PIXELS` is the same cap. */
    val StripPixels: Int = 1048576

    /** True when some pixel of `ds` is valid ([[ValidMask]]). */
    def anyValid(ds: Dataset): Boolean = new ValidMask(ds).anyValid()
}
