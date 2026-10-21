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

    /** The NoData state of `ds` for a warp: the bands that declare none, and the value list gdalwarp takes
      * (`-srcnodata` / `-dstnodata`): one value per band, a declared band's own value or `fill(band)`, written as
      * ONE value when they all agree. The single scan of the bands' declared NoData that the warp helpers share. */
    private final case class WarpNoData(undeclared: Seq[org.gdal.gdal.Band], list: String)

    private def warpNoData(ds: org.gdal.gdal.Dataset, fill: org.gdal.gdal.Band => Double): WarpNoData = {
        val perBand = (1 to ds.GetRasterCount()).map { i =>
            val band = ds.GetRasterBand(i)
            val nd = new Array[java.lang.Double](1)
            band.GetNoDataValue(nd)
            (Option(nd(0)).map(_.doubleValue()), band)
        }
        val values = perBand.map { case (declared, band) => declared.getOrElse(fill(band)) }
        val list = values.distinct match {
            case Seq(single) => single.toString
            case _           => values.mkString(" ")
        }
        WarpNoData(perBand.collect { case (None, band) => band }, list)
    }

    /** True when `band` is masked by its own per-dataset mask band that is not an alpha band (an internal or
      * external GTiff mask, for example). gdalwarp ignores such a mask once a source NoData is set, and under
      * a cutline it has to rasterize; it always applies an alpha band. */
    def hasOwnMask(band: org.gdal.gdal.Band): Boolean = {
        val flags = band.GetMaskFlags()
        (flags & GMF_PER_DATASET) != 0 && (flags & GMF_ALPHA) == 0
    }

    /** True when `ds` has an alpha band, or a band masked by one: gdalwarp carries the alpha band and
      * applies it as the source mask. */
    def hasAlpha(ds: org.gdal.gdal.Dataset): Boolean =
        (1 to ds.GetRasterCount()).map(ds.GetRasterBand).exists { b =>
            b.GetColorInterpretation == GCI_AlphaBand || (b.GetMaskFlags() & GMF_ALPHA) != 0
        }

    /**
      * gdalwarp arguments that make the source pixels equal to a band's fill value NoData, for the bands of
      * `ds` that declare no NoData: `-srcnodata <one value per band>` (a declared band keeps its own value)
      * and `-wo UNIFIED_SRC_NODATA=NO`, so each band is judged on its own (GDAL otherwise treats a pixel
      * as NoData only when every band matches). Without them, gdalwarp `-dstnodata X` moves a valid source
      * pixel equal to X to X +/- 1 and keeps it valid. `fill` gives an undeclared band's value, by default
      * [[getNoDataConstant]] for its type. Empty when every band declares NoData, or when a band without
      * NoData has its own mask ([[hasOwnMask]]): a source NoData makes gdalwarp drop that mask, so masked
      * pixels would turn valid (an alpha band still masks). `maskAsAlpha` says the warp reads that mask as
      * a source alpha band instead (the mask view of [[com.databricks.labs.gbx.rasterx.operations.ClipToGeom]]),
      * which a source NoData does not drop, so the arguments are given then. The list is ONE argument: pass
      * it as a warp extra argument, not through a space-split command string.
      */
    def srcNoDataArgs(
        ds: org.gdal.gdal.Dataset,
        fill: org.gdal.gdal.Band => Double = b => getNoDataConstant(b.getDataType),
        maskAsAlpha: Boolean = false
    ): Seq[String] = {
        val nd = warpNoData(ds, fill)
        if (nd.undeclared.isEmpty || (!maskAsAlpha && nd.undeclared.exists(hasOwnMask))) Seq.empty
        else Seq("-srcnodata", nd.list, "-wo", "UNIFIED_SRC_NODATA=NO")
    }

    /**
      * gdalwarp `-dstnodata <one value per band>` for `ds`, when some band declares no NoData: an undeclared
      * band gets `fill(band)` (default [[getNoDataConstant]]), a declared band keeps its own value. Empty when
      * every band declares NoData: the warp carries it. Pair it with [[srcNoDataArgs]]; `-dstnodata` alone
      * moves source pixels equal to the value by +/- 1 and keeps them valid. ONE argument, like there.
      */
    def dstNoDataArgs(
        ds: org.gdal.gdal.Dataset,
        fill: org.gdal.gdal.Band => Double = b => getNoDataConstant(b.getDataType)
    ): Seq[String] = {
        val nd = warpNoData(ds, fill)
        if (nd.undeclared.isEmpty) Seq.empty else Seq("-dstnodata", nd.list)
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
