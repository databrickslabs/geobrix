package com.databricks.labs.gbx.rasterx.operator

import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants._

/** Parses GDAL command strings and appends format/compression/block options for translate/warp/calc. */
object OperatorOptions {

    /**
      * Codec used when a caller supplies no `compression` option. Shared by [[appendOptions]] (the `-co`
      * flags) and by every operation that stamps `compression` into the tile metadata it returns, so the
      * metadata a derived tile carries can never disagree with the codec GDAL actually writes.
      *
      * DEFLATE (with the dtype-matched predictor) is baseline TIFF: Java ImageIO and the Databricks image
      * preview can decode it, which ZSTD output is not. `rst_cog_convert` keeps its own documented ZSTD
      * default ([[com.databricks.labs.gbx.rasterx.expressions.analysis.RST_CogConvert.CogDefaultCompression]]).
      */
    val DefaultCompression: String = "DEFLATE"

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
        val compression = writeOptions.getOrElse("compression", DefaultCompression).toUpperCase
        // scalastyle:on caselocale
        val missingGeoRef = writeOptions.getOrElse("missingGeoRef", "false").toBoolean
        val isCalc = command.startsWith("gdal_calc")
        val ofFlag = if (isCalc) "--format" else "-of"
        val coFlag = if (isCalc) "--co" else "-co"

        // Compute predictor based on dtype: 3 for float, 1 for uint8/int8, 2 for others.
        // Mirrors pyrx.core.compression._FLOAT, _SMALL_INT predictor mapping.
        //
        // GDAL applies the TIFF predictor to the OUTPUT raster, so it must match the
        // output datatype (PREDICTOR=3 is Float32/Float64-only). When the command sets an
        // explicit output type — gdal_translate "-ot <Type>", gdal_calc "--type <Type>" or
        // "--type=<Type>" — derive the predictor from that type; otherwise fall back to the
        // input band types. Without this, converting a Float raster to Byte (RST_UpdateType)
        // derived PREDICTOR=3 from the float input and gdal_translate rejected the Byte
        // output. Both the space and equals forms are accepted because the repo's gdal_calc
        // callers emit "--type=Float32" while gdal_translate emits "-ot Byte". (#82)
        def predictorForType(dt: Int): String =
            if (dt == GDT_Float32 || dt == GDT_Float64) "3"
            else if (dt == GDT_Byte || dt == GDT_Int8) "1"
            else "2"
        val outputTypeName: Option[String] = {
            val toks = command.split(" ").toSeq
            toks.zipWithIndex.collectFirst {
                case (t, i) if (t == "-ot" || t == "--type") && i + 1 < toks.length => toks(i + 1)
                case (t, _) if t.startsWith("-ot=")   => t.substring(4)
                case (t, _) if t.startsWith("--type=") => t.substring(7)
            }
        }
        val outputType: Int = outputTypeName.map(gdal.GetDataTypeByName).getOrElse(GDT_Unknown)
        val predictor =
            if (outputType != GDT_Unknown) {
                predictorForType(outputType)
            } else {
                // No explicit output type -> derive from input bands, reusing the single
                // dtype->predictor mapping above (float wins, then Byte/Int8, else 2).
                val perBand = (1 to ds.GetRasterCount).map(i => predictorForType(ds.GetRasterBand(i).GetRasterDataType))
                if (perBand.contains("3")) "3" else if (perBand.contains("1")) "1" else "2"
            }

        val w = ds.GetRasterXSize; val h = ds.GetRasterYSize
        val rawBlk = math.max(64, math.min(writeOptions.getOrElse("blocksize", "512").toInt, math.min(w, h)))
        val blk = (rawBlk / 16) * 16 // floor to nearest multiple of 16

        val coBase = format match {
            case "COG"   => Seq(s"$coFlag BLOCKSIZE=$blk")
            case "GTiff" => Seq(s"$coFlag TILED=YES", s"$coFlag BLOCKXSIZE=$blk", s"$coFlag BLOCKYSIZE=$blk", s"$coFlag BIGTIFF=IF_SAFER")
            case _       => Seq.empty
        }

        val coComp = compression match {
            case "ZSTD"    => {
                // COG driver uses "LEVEL"; GTiff uses "ZSTD_LEVEL"
                val levelOption = if (format == "COG") "LEVEL" else "ZSTD_LEVEL"
                val levelKey = if (format == "COG") "level" else "zstd_level"
                Seq(s"$coFlag COMPRESS=ZSTD", s"$coFlag $levelOption=${writeOptions.getOrElse(levelKey, "9")}", s"$coFlag PREDICTOR=$predictor")
            }
            case "DEFLATE" => {
                // COG driver uses "LEVEL"; GTiff uses "ZLEVEL"
                val levelOption = if (format == "COG") "LEVEL" else "ZLEVEL"
                val levelKey = if (format == "COG") "level" else "zlevel"
                Seq(
                    s"$coFlag COMPRESS=DEFLATE",
                    s"$coFlag PREDICTOR=$predictor",
                    s"$coFlag $levelOption=${writeOptions.getOrElse(levelKey, "6")}"
                )
            }
            case "LZW"     => Seq(s"$coFlag COMPRESS=LZW", s"$coFlag PREDICTOR=$predictor")
            case other     => Seq(s"$coFlag COMPRESS=$other")
        }

        val cos = (coBase ++ coComp).mkString(" ")

        // Optional per-band rescale string (e.g. "-scale_1 min max 0 255 -scale_2 ..."),
        // supplied by the XYZ tilers for data-aware 8-bit encoding. Empty/absent => no -scale
        // (today's full-dtype-range behavior). See RST_TileXYZ rescale resolution.
        // Note: rescale currently affects the PNG byte-output branch only; JPEG/WEBP rescale
        // is a documented future follow-up.
        val scaleFlags = writeOptions.getOrElse("scale", "").trim
        val scaleSuffix = if (scaleFlags.isEmpty) "" else s" $scaleFlags"

        format match {
            case _ if command.startsWith("gdalbuildvrt") => command // VRT does not require additional options
            case "VRT"                                   => command
            case "PNM" if isCalc                         => s"$command $ofFlag $format"
            case "PNM"                                   => s"$command $ofFlag $format -ot UInt16 -scale -32768 32767 0 65535"
            case "PNG"                                   => s"$command $ofFlag $format -ot Byte -a_nodata none$scaleSuffix" // PNG Byte format, strip NoData to avoid tRNS issues
            case "Zarr" if missingGeoRef                 => s"$command $ofFlag $format -to SRC_METHOD=NO_GEOTRANSFORM $cos"
            case f                                       => s"$command $ofFlag $f $cos"
        }
    }

}
