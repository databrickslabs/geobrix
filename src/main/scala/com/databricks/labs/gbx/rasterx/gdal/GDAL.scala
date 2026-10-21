package com.databricks.labs.gbx.rasterx.gdal

import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants._
import org.gdal.osr.SpatialReference

/** GDAL constants and helpers: WGS84/3857 refs, driver extension, NoData by type, pixel-to/from-geo coordinate conversion. */
object GDAL {

    /** WGS84 spatial reference (EPSG:4326, traditional axis order). Note: val name is WSG84 (historical typo). */
    val WSG84: SpatialReference = {
        val wsg84 = new SpatialReference()
        wsg84.ImportFromEPSG(4326)
        wsg84.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        wsg84
    }

    /** Web Mercator spatial reference (EPSG:3857). */
    val EPSG3857: SpatialReference = {
        val epsg3857 = new SpatialReference()
        epsg3857.ImportFromEPSG(3857)
        epsg3857.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        epsg3857
    }

    /** File extension for the driver (e.g. tif for GTiff); uses FormatLookup when DMD_EXTENSION is null. Driver is deleted before return. */
    def getExtension(driverShortName: String): String = {
        val driver = gdal.GetDriverByName(driverShortName)
        val result = driver.GetMetadataItem("DMD_EXTENSION")
        val toReturn = if (result == null) FormatLookup.formats(driverShortName) else result
        driver.delete()
        toReturn
    }

    /** Returns a NoData sentinel value for the given GDAL data type constant. */
    def getNoDataConstant(gdalType: Int): Double = {
        gdalType match {
            case GDT_Unknown => 0.0
            case GDT_Byte    => 0.0
            // Unsigned Int16 is Char in scala
            // https://www.tutorialspoint.com/scala/scala_data_types.htm
            case GDT_UInt16  => Char.MaxValue.toDouble
            case GDT_Int16   => Short.MinValue.toDouble
            case GDT_UInt32  => 2 * Int.MaxValue.toDouble
            case GDT_Int32   => Int.MinValue.toDouble
            case GDT_Float32 => Float.MinValue.toDouble
            case GDT_Float64 => Double.MinValue
            case _           => 0.0
        }
    }

    /**
      * gdalwarp arguments that make the source pixels equal to a band's fill value NoData, for the bands of
      * `ds` that declare no NoData: `-srcnodata <one value per band>` (a declared band keeps its own value)
      * and `-wo UNIFIED_SRC_NODATA=NO`, so each band is judged on its own (GDAL otherwise treats a pixel
      * as NoData only when every band matches). Without them, gdalwarp `-dstnodata X` moves a valid source
      * pixel equal to X to X +/- 1 and keeps it valid. `fill` gives an undeclared band's value, by default
      * [[getNoDataConstant]] for its type. Empty when every band declares NoData. The list is ONE argument:
      * pass it as a warp extra argument, not through a space-split command string.
      */
    def srcNoDataArgs(
        ds: org.gdal.gdal.Dataset,
        fill: org.gdal.gdal.Band => Double = b => getNoDataConstant(b.getDataType)
    ): Seq[String] = {
        val perBand = (1 to ds.GetRasterCount()).map { i =>
            val band = ds.GetRasterBand(i)
            val nd = new Array[java.lang.Double](1)
            band.GetNoDataValue(nd)
            (Option(nd(0)).map(_.doubleValue()), band)
        }
        if (perBand.forall(_._1.isDefined)) Seq.empty
        else {
            val values = perBand.map { case (declared, band) => declared.getOrElse(fill(band)) }
            val list = values.distinct match {
                case Seq(single) => single.toString
                case _           => values.mkString(" ")
            }
            Seq("-srcnodata", list, "-wo", "UNIFIED_SRC_NODATA=NO")
        }
    }

    /** Converts pixel (x, y) to geographic (xGeo, yGeo) using the 6-element geotransform. */
    def toWorldCoord(gt: Array[Double], x: Int, y: Int): (Double, Double) = {
        val offset = 0.5 // offset to center of the pixel
        val xGeo = gt(0) + (x + offset) * gt(1) + (y + offset) * gt(2)
        val yGeo = gt(3) + (x + offset) * gt(4) + (y + offset) * gt(5)
        (xGeo, yGeo)
    }

    /** Converts geographic (xGeo, yGeo) to pixel (x, y) using the 6-element geotransform. */
    def fromWorldCoord(gt: Array[Double], xGeo: Double, yGeo: Double): (Int, Int) = {
        val det = gt(1) * gt(5) - gt(2) * gt(4)
        val dx = xGeo - gt(0)
        val dy = yGeo - gt(3)
        val x = ( dx * gt(5) - dy * gt(2)) / det
        val y = (-dx * gt(4) + dy * gt(1)) / det
        (x.toInt, y.toInt)
    }

}
