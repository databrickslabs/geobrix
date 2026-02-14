package com.databricks.labs.gbx.rasterx.operations

import org.gdal.gdal.Band
import org.gdal.gdalconst.gdalconstConstants

import scala.jdk.CollectionConverters.DictionaryHasAsScala
import scala.util.Try

/** Extracts metadata and statistics from a GDAL band (metadata dict, min/max, NoData, data type, mask). */
object BandAccessors {

    /** Returns the band's metadata dictionary as a String->String map. */
    def getMetadata(band: Band): Map[String, String] = {
        if (Option(band).isEmpty) Map.empty
        else {
            Option(band.GetMetadata_Dict())
                .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
                .getOrElse(Map.empty[String, String])
        }
    }

    /** Computes and returns (min, max) pixel value for the band; (NaN, NaN) if band is null. */
    def getMinMax(band: Band): (Double, Double) = {
        val minmax = Array.ofDim[Double](2)
        if (Option(band).isEmpty) (Double.NaN, Double.NaN)
        else {
            band.ComputeRasterMinMax(minmax, 0)
            (minmax(0), minmax(1))
        }
    }

    /** Returns the band's NoData value, or Double.NaN if unset or band is null. */
    def getNoDataValue(band: Band): Double = {
        if (Option(band).isEmpty) Double.NaN
        else {
            val noDataVal = Array.fill[java.lang.Double](1)(0)
            band.GetNoDataValue(noDataVal)
            if (noDataVal(0) == null || noDataVal(0).isNaN) Double.NaN
            else {
                val noDataValue = noDataVal(0).doubleValue()
                if (noDataValue == Double.NaN) Double.NaN
                else noDataValue
            }
        }
    }

    /** Returns a human-readable data type name (e.g. Byte, Float32) for the band. */
    def dataTypeHuman(band: Band): String =
        Try(band.getDataType).getOrElse(0) match {
            case gdalconstConstants.GDT_Byte     => "Byte"
            case gdalconstConstants.GDT_UInt16   => "UInt16"
            case gdalconstConstants.GDT_Int16    => "Int16"
            case gdalconstConstants.GDT_UInt32   => "UInt32"
            case gdalconstConstants.GDT_Int32    => "Int32"
            case gdalconstConstants.GDT_Float32  => "Float32"
            case gdalconstConstants.GDT_Float64  => "Float64"
            case gdalconstConstants.GDT_CInt16   => "ComplexInt16"
            case gdalconstConstants.GDT_CInt32   => "ComplexInt32"
            case gdalconstConstants.GDT_CFloat32 => "ComplexFloat32"
            case gdalconstConstants.GDT_CFloat64 => "ComplexFloat64"
            case _                               => "Unknown"
        }

    /** Returns true if the band has no valid pixels (mask all invalid). */
    def isEmpty(band: Band): Boolean = {
        val flags = band.GetMaskFlags()
        if ((flags & gdalconstConstants.GMF_ALL_VALID) != 0) return false
        val mask = band.GetMaskBand()
        if (Option(mask).isEmpty) return true

        val w = band.GetXSize()
        val h = band.GetYSize()
        val bW = mask.GetBlockXSize()
        val bH = mask.GetBlockYSize()
        val buffer = java.nio.ByteBuffer.allocateDirect(bW * bH)

        var y = 0
        while (y < h) {
            val rh = math.min(bH, h - y)
            var x = 0
            while (x < w) {
                val rw = math.min(bW, w - x)
                buffer.clear()
                buffer.limit(rw * rh)
                mask.ReadRaster_Direct(x, y, rw, rh, buffer)
                var i = 0; val lim = rw * rh
                while (i < lim) { if ((buffer.get(i) & 0xff) != 0) return false; i += 1 }
                x += bW
            }
            y += bH
        }
        true
    }

}
