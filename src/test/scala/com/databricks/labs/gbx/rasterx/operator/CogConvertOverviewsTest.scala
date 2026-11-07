package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.expressions.analysis.RST_CogConvert
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.util.RasterSerializationUtil
import org.apache.spark.sql.types.BinaryType
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

/**
  * `rst_cog_convert` keeps the overview pyramid when its result is encoded, as the lightweight function's COG has it.
  */
class CogConvertOverviewsTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit =
        GDALManager.init(ExpressionConfig(Map.empty[String, String], new SerializableConfiguration(new Configuration())))

    private def mem(name: String): String = s"/vsimem/l1pf_${UUID.randomUUID().toString.replace("-", "")}_$name"

    /** A Float64 raster of `values` (row-major), top-left origin `(0, h * dy)`, `dx` by `dy` cells, in EPSG:32633. */
    private def raster(w: Int, h: Int, dx: Double, dy: Double, values: Array[Double]): Dataset = {
        val ds = gdal.GetDriverByName("MEM").Create("", w, h, 1, gdalconstConstants.GDT_Float64)
        ds.SetGeoTransform(Array(0.0, dx, 0.0, h * dy, 0.0, -dy))
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(32633)
        ds.SetProjection(sr.ExportToWkt())
        ds.GetRasterBand(1).WriteRaster(0, 0, w, h, values)
        ds
    }

    private def pixels(ds: Dataset): Array[Double] = {
        val buf = new Array[Double](ds.GetRasterXSize * ds.GetRasterYSize)
        ds.GetRasterBand(1).ReadRaster(0, 0, ds.GetRasterXSize, ds.GetRasterYSize, buf)
        buf
    }

    private def open(bytes: Array[Byte]): Dataset = {
        val p = mem("read.tif")
        gdal.FileFromMemBuffer(p, bytes)
        gdal.Open(p)
    }

    // ---- rst_cog_convert: overviews ------------------------------------------------------------------------------

    test("rst_cog_convert keeps its overview pyramid when the result is encoded") {
        val (w, h) = (512, 512)
        val values = Array.tabulate(w * h)(i => ((i % w) + (i / w) % 17).toDouble)
        val src = raster(w, h, 1.0, 1.0, values)
        val hconf = new SerializableConfiguration(new Configuration())
        try {
            val (res, mtd) = RST_CogConvert.execute(src, Map.empty, "DEFLATE", 128, "AVERAGE")
            // what the expression does with the result (RST_CogConvert.runChecked)
            val row = RasterSerializationUtil.tileToRow(
                (0L, res, mtd), BinaryType, hconf, None, RST_CogConvert.blockWrite(128, "AVERAGE"))
            val bytes = row.getBinary(com.databricks.labs.gbx.rasterx.util.V2Tile.idx("raster"))
            val back = open(bytes)
            // the light tier's COG (GDAL's COG driver, same block size and resampling) is the policy
            val ref = gdal.Translate(mem("ref.tif"), src, new org.gdal.gdal.TranslateOptions(
                new java.util.Vector[String](java.util.Arrays.asList(
                    "-of", "COG", "-co", "BLOCKSIZE=128", "-co", "COMPRESS=DEFLATE", "-co", "OVERVIEW_RESAMPLING=AVERAGE"))))
            try {
                val overviews = back.GetRasterBand(1).GetOverviewCount
                assert(overviews >= 1 && overviews == ref.GetRasterBand(1).GetOverviewCount,
                    s"overviews: $overviews, GDAL COG driver: ${ref.GetRasterBand(1).GetOverviewCount}")
                assert(back.GetMetadataItem("LAYOUT", "IMAGE_STRUCTURE") == "COG")
                assert(back.GetMetadataItem("COMPRESSION", "IMAGE_STRUCTURE") == "DEFLATE")
                assert(pixels(back).sameElements(values), "pixels changed")
            } finally { RasterDriver.releaseDataset(back); ref.delete(); RasterDriver.releaseDataset(res) }
        } finally src.delete()
    }

}
