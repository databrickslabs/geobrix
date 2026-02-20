package com.databricks.labs.gbx.rasterx.operator

import org.gdal.gdal.Dataset
import org.gdal.gdalconst.gdalconstConstants._

/** Parses GDAL command strings and appends format/compression/block options for translate/warp/calc. */
object OperatorOptions {

    /** Splits command by spaces and returns options as Vector (args after first token). */
    def parseOptions(command: String): java.util.Vector[String] = {
        val args = command.split(" ")
        val optionsVec = new java.util.Vector[String]()
        args.drop(1).foreach(optionsVec.add)
        optionsVec
    }

    /** Appends -of/--format, -co compression/blocksize/predictor from writeOptions and ds; does not change output format (use RST_AsFormat for that). */
    def appendOptions(command: String, writeOptions: Map[String, String], ds: Dataset): String = {
        val format = writeOptions.getOrElse("format", "GTiff")
        // scalastyle:off caselocale
        val compression = writeOptions.getOrElse("compression", "ZSTD").toUpperCase
        // scalastyle:on caselocale
        val missingGeoRef = writeOptions.getOrElse("missingGeoRef", "false").toBoolean
        val isCalc = command.startsWith("gdal_calc")
        val ofFlag = if (isCalc) "--format" else "-of"
        val coFlag = if (isCalc) "--co" else "-co"

        val anyFloat = (1 to ds.GetRasterCount).exists { i =>
            val dt = ds.GetRasterBand(i).GetRasterDataType
            dt == GDT_Float32 || dt == GDT_Float64
        }
        val predictor = if (anyFloat) "3" else "2"

        val w = ds.GetRasterXSize; val h = ds.GetRasterYSize
        val rawBlk = math.max(64, math.min(writeOptions.getOrElse("blocksize", "512").toInt, math.min(w, h)))
        val blk = (rawBlk / 16) * 16 // floor to nearest multiple of 16

        val coBase = format match {
            case "COG"   => Seq(s"$coFlag BLOCKSIZE=$blk")
            case "GTiff" => Seq(s"$coFlag TILED=YES", s"$coFlag BLOCKXSIZE=$blk", s"$coFlag BLOCKYSIZE=$blk", s"$coFlag BIGTIFF=IF_SAFER")
            case _       => Seq.empty
        }

        val coComp = compression match {
            case "ZSTD"    => Seq(s"$coFlag COMPRESS=ZSTD", s"$coFlag ZSTD_LEVEL=${writeOptions.getOrElse("zstd_level", "9")}")
            case "DEFLATE" => Seq(
                  s"$coFlag COMPRESS=DEFLATE",
                  s"$coFlag PREDICTOR=$predictor",
                  s"$coFlag ZLEVEL=${writeOptions.getOrElse("zlevel", "6")}"
                )
            case "LZW"     => Seq(s"$coFlag COMPRESS=LZW", s"$coFlag PREDICTOR=$predictor")
            case other     => Seq(s"$coFlag COMPRESS=$other")
        }

        val cos = (coBase ++ coComp).mkString(" ")

        format match {
            case _ if command.startsWith("gdalbuildvrt") => command // VRT does not require additional options
            case "VRT"                                   => command
            case "PNM" if isCalc                         => s"$command $ofFlag $format"
            case "PNM"                                   => s"$command $ofFlag $format -ot UInt16 -scale -32768 32767 0 65535"
            case "PNG"                                   => s"$command $ofFlag $format -ot Byte -a_nodata none" // PNG Byte format, strip NoData to avoid tRNS issues
            case "Zarr" if missingGeoRef                 => s"$command $ofFlag $format -to SRC_METHOD=NO_GEOTRANSFORM $cos"
            case f                                       => s"$command $ofFlag $f $cos"
        }
    }

}
