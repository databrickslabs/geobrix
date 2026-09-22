package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants._

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
  * Per-band percentile-based contrast stretching to uint8.
  *
  * For each band:
  *   1. Read all pixel values (excluding NoData) into memory
  *   2. Compute lo_pct and hi_pct percentiles
  *   3. Create output uint8 dataset
  *   4. Rescale and write pixels: new = ((old - lo_pct_val) / (hi_pct_val - lo_pct_val)) * 255
  *   5. Clip to [0, 255] and convert to uint8
  *   6. NoData pixels output as 0
  */
object PercentileStretch {

    /** Executes percentile stretch on a dataset and returns a new uint8 GTiff. */
    def execute(
        ds: Dataset, options: Map[String, String], loPct: Double, hiPct: Double
    ): (Dataset, Map[String, String]) = {
        require(ds != null, "PercentileStretch.execute: source Dataset is null")

        val nBands = ds.GetRasterCount
        val width = ds.GetRasterXSize
        val height = ds.GetRasterYSize

        // Create output uint8 dataset (MEM driver first, then copy to GTiff)
        val memDriver = gdal.GetDriverByName("MEM")
        val outDs = memDriver.Create("", width, height, nBands, GDT_Byte)

        try {
            // Copy geotransform and projection
            outDs.SetGeoTransform(ds.GetGeoTransform)
            val proj = ds.GetProjectionRef
            if (proj != null && proj.nonEmpty) {
                outDs.SetProjection(proj)
            }

            // Process each band
            var b = 1
            while (b <= nBands) {
                val inBand = ds.GetRasterBand(b)
                val outBand = outDs.GetRasterBand(b)

                // Get NoData value if set
                val noDataValue: Option[Double] = {
                    val noDataArray = new Array[java.lang.Double](1)
                    inBand.GetNoDataValue(noDataArray)
                    if (noDataArray(0) != null) Option(noDataArray(0).doubleValue()) else None
                }

                // Read pixels and compute percentiles
                val (pixelValues, stretchLoVal, stretchHiVal) = computePercentiles(
                    inBand, width, height, noDataValue, loPct, hiPct
                )

                // Rescale and write output
                rescaleAndWrite(
                    inBand, outBand, width, height, noDataValue,
                    stretchLoVal, stretchHiVal
                )

                outBand.FlushCache()
                b += 1
            }

            outDs.FlushCache()

            // Convert from MEM to GTiff via gdal_translate
            val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
            val extension = GDAL.getExtension("GTiff")
            val outPath = s"/vsimem/percentile_stretch_$uuid.$extension"
            val (resultDs, mtd) = com.databricks.labs.gbx.rasterx.operator.GDALTranslate.executeTranslate(
                outPath, outDs, "gdal_translate", options
            )
            resultDs.FlushCache()

            (resultDs, mtd)
        } finally {
            RasterDriver.releaseDataset(outDs)
        }
    }

    /** Compute percentile values from a band, excluding NoData pixels. */
    private def computePercentiles(
        band: org.gdal.gdal.Band,
        width: Int,
        height: Int,
        noDataValue: Option[Double],
        loPct: Double,
        hiPct: Double
    ): (Seq[Double], Double, Double) = {
        val pixelValues = mutable.ArrayBuffer[Double]()

        // Read all pixels into memory
        val pixelData = new Array[Double](width * height)
        band.ReadRaster(0, 0, width, height, pixelData)

        // Filter out NoData values
        var i = 0
        while (i < pixelData.length) {
            val pv = pixelData(i)
            val isNoData = noDataValue.exists { nd =>
                // Handle floating-point comparison with tolerance
                math.abs(pv - nd) < 1e-10
            }
            if (!isNoData && !pv.isNaN) {
                pixelValues += pv
            }
            i += 1
        }

        // Compute percentiles
        val sortedValues = pixelValues.sorted
        val loIdx = math.max(0, math.round((loPct / 100.0) * (sortedValues.length - 1)).toInt)
        val hiIdx = math.min(sortedValues.length - 1, math.round((hiPct / 100.0) * (sortedValues.length - 1)).toInt)

        val loVal = if (sortedValues.nonEmpty) sortedValues(loIdx) else 0.0
        val hiVal = if (sortedValues.nonEmpty) sortedValues(hiIdx) else 255.0

        (pixelValues.toSeq, loVal, hiVal)
    }

    /** Rescale input band pixels to uint8 and write to output band. */
    private def rescaleAndWrite(
        inBand: org.gdal.gdal.Band,
        outBand: org.gdal.gdal.Band,
        width: Int,
        height: Int,
        noDataValue: Option[Double],
        stretchLoVal: Double,
        stretchHiVal: Double
    ): Unit = {
        val pixelData = new Array[Double](width * height)
        inBand.ReadRaster(0, 0, width, height, pixelData)

        val outPixelData = new Array[Byte](width * height)
        val range = stretchHiVal - stretchLoVal

        var i = 0
        while (i < pixelData.length) {
            val pv = pixelData(i)
            val isNoData = noDataValue.exists { nd =>
                math.abs(pv - nd) < 1e-10
            }

            val byteValue: Byte = if (isNoData || pv.isNaN) {
                0.toByte
            } else {
                // Rescale from [stretchLoVal, stretchHiVal] to [0, 255]
                val clipped = math.max(stretchLoVal, math.min(stretchHiVal, pv))
                val normalized = if (range > 0) (clipped - stretchLoVal) / range else 0.0
                val rescaled = normalized * 255.0
                val clamped = math.max(0, math.min(255, math.round(rescaled).toInt))
                clamped.toByte
            }
            outPixelData(i) = byteValue
            i += 1
        }

        outBand.WriteRaster(0, 0, width, height, outPixelData)
    }

}
