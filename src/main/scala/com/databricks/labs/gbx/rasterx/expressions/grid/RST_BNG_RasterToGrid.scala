package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference

import scala.collection.mutable

/** Shared helper for `RST_BNG_RasterToGrid*` expressions — mirrors `RST_Quadbin_RasterToGrid`
  * but delegates per-pixel cell math to [[BNG.pointToCellID]] (EPSG:27700 eastings/northings).
  *
  * Unlike the H3/quadbin families (whose input contract is EPSG:4326 lon/lat), BNG has no lon/lat
  * input path, so the raster is reprojected to EPSG:27700 up front using nearest-neighbour
  * resampling (`gdalwarp -t_srs EPSG:27700 -r near`). Cell ids are `Long` internally and rendered
  * to the user-facing BNG `String` via [[BNG.format]] at the output boundary. Pixels outside the
  * GB extent map to out-of-range BNG ids and are silently discarded by the mask check.
  *
  * Resampling is hard-coded to `near` because raster→grid aggregation is a pixel-counting
  * operation — any interpolating kernel would fabricate pixel values and corrupt statistics.
  */
object RST_BNG_RasterToGrid {

    /** Compute the BNG cell id for the centroid of pixel (x, y) under geotransform `gt` (EPSG:27700). */
    def cellPixel(gt: Array[Double], x: Int, y: Int, resolution: Int): Long = {
        val offset = 0.5
        val xOffset = offset + x
        val yOffset = offset + y
        val eGeo = gt(0) + xOffset * gt(1) + yOffset * gt(2)
        val nGeo = gt(3) + xOffset * gt(4) + yOffset * gt(5)
        BNG.pointToCellID(eGeo, nGeo, resolution)
    }

    /** Reproject `ds` to EPSG:27700 using nearest-neighbour resampling. Caller must release the returned Dataset. */
    private def warpToBNG(ds: Dataset): Dataset = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val driver = ds.GetDriver()
        val extension = GDAL.getExtension(driver.getShortName)
        val resultPath = s"/vsimem/raster_bng_$uuid.$extension"
        val (result, _) = GDALWarp.executeWarp(
          resultPath,
          Array(ds),
          Map.empty[String, String],
          command = "gdalwarp -t_srs EPSG:27700 -r near"
        )
        result
    }

    def execute[T](
        ds: Dataset,
        resolution: Int,
        fAgg: mutable.ArrayBuffer[Double] => T
    ): Array[Array[(String, T)]] = {
        require(
          BNG.resolutions.contains(resolution),
          s"raster→bng: resolution must be one of ${BNG.resolutions.toSeq.sorted.mkString(", ")}; got $resolution"
        )

        // Reproject to EPSG:27700 (nearest-neighbour) unless already there.
        val dstSR = new SpatialReference(); dstSR.ImportFromEPSG(27700)
        val srcWkt = ds.GetProjection()
        val alreadyBNG = srcWkt != null && srcWkt.nonEmpty && {
            val s = new SpatialReference()
            s.ImportFromWkt(srcWkt)
            val same = s.IsSame(dstSR) == 1
            s.delete()
            same
        }
        dstSR.delete()

        val (workDs, reprojected) =
            if (alreadyBNG) (ds, false)
            else (warpToBNG(ds), true)

        try {
            val gt = workDs.GetGeoTransform
            val xSize = workDs.getRasterXSize
            val ySize = workDs.getRasterYSize
            val nPix = xSize * ySize
            val bands = workDs.getRasterCount

            val bandBuf = new Array[Double](nPix)
            val maskBuf = new Array[Byte](nPix)

            (1 to bands).iterator.map { bi =>
                val b = workDs.GetRasterBand(bi)
                val m = b.GetMaskBand()
                b.ReadRaster(0, 0, xSize, ySize, bandBuf)
                m.ReadRaster(0, 0, xSize, ySize, maskBuf)

                var valid = 0; var i = 0
                while (i < nPix) { if (maskBuf(i) != 0) valid += 1; i += 1 }

                val acc = new mutable.LongMap[mutable.ArrayBuffer[Double]](valid)
                var y = 0; var idx = 0
                while (y < ySize) {
                    var x = 0
                    while (x < xSize) {
                        if (maskBuf(idx) != 0) {
                            val cell = cellPixel(gt, x, y, resolution) // Long id
                            val buf = acc.getOrElseUpdate(cell, new mutable.ArrayBuffer)
                            buf += bandBuf(idx)
                        }
                        idx += 1; x += 1
                    }
                    y += 1
                }

                val out = new Array[(String, T)](acc.size)
                var j = 0
                acc.foreach { case (cell, buf) => out(j) = (BNG.format(cell), fAgg(buf)); j += 1 }
                out
            }.toArray
        } finally {
            if (reprojected) RasterDriver.releaseDataset(workDs)
        }
    }

    def eval[T](
        row: InternalRow,
        resolution: Int,
        conf: UTF8String,
        rdt: DataType,
        execute: (Dataset, Int) => Array[Array[(String, T)]]
    ): ArrayData = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val result = execute(ds, resolution)
        RasterDriver.releaseDataset(ds)
        ArrayData.toArrayData(
          result.map(band =>
              ArrayData.toArrayData(
                band.map { case (cellId, measure) =>
                    InternalRow.fromSeq(Seq(UTF8String.fromString(cellId), measure))
                }
              )
          )
        )
    }
}
