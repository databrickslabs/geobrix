package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.grid.Quadbin
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.expressions.grid.GridReprojection
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import scala.collection.mutable

/** Shared helper for `RST_Quadbin_RasterToGrid*` expressions — mirrors `RST_H3_RasterToGrid`
  * but delegates per-pixel cell math to [[Quadbin.pointToCell]] (CARTO quadbin v0).
  *
  * The geotransform interprets the raster as EPSG:4326 lon/lat; a differently-CRS'd raster
  * is auto-reprojected to 4326 (nearest-neighbour) up front via [[GridReprojection.toGridCrs]],
  * so easting/northing are never read as lon/lat. A CRS-less raster is assumed already-4326.
  *
  * Resolution range: [0, 20]. Capped well below the CARTO v0 max of 26 because the
  * per-band cell count at z>=21 over a continental raster (~10^6) is dominated by GDAL I/O
  * and easily OOMs.
  */
object RST_Quadbin_RasterToGrid {

    /** Maximum quadbin resolution permitted for raster→grid aggregation. */
    val MAX_AGG_RESOLUTION: Int = 20

    /** Compute the quadbin cell id for the centroid of pixel (x, y) under geotransform `gt`. */
    def cellPixel(gt: Array[Double], x: Int, y: Int, resolution: Int): Long = {
        val offset = 0.5 // center of pixel
        val xOffset = offset + x
        val yOffset = offset + y
        val xGeo = gt(0) + xOffset * gt(1) + yOffset * gt(2)
        val yGeo = gt(3) + xOffset * gt(4) + yOffset * gt(5)
        Quadbin.pointToCell(xGeo, yGeo, resolution)
    }

    def execute[T](
        ds: Dataset,
        resolution: Int,
        fAgg: mutable.ArrayBuffer[Double] => T
    ): Array[Array[(Long, T)]] = {
        require(
          resolution >= 0 && resolution <= MAX_AGG_RESOLUTION,
          s"raster→quadbin: resolution must be in [0, $MAX_AGG_RESOLUTION]; got $resolution"
        )
        // Auto-reproject to grid-native EPSG:4326 (nearest) unless already there or CRS-less.
        val (workDs, reprojected) = GridReprojection.toGridCrs(ds, 4326)
        try executeOn(workDs, resolution, fAgg)
        finally if (reprojected) RasterDriver.releaseDataset(workDs)
    }

    private def executeOn[T](
        ds: Dataset,
        resolution: Int,
        fAgg: mutable.ArrayBuffer[Double] => T
    ): Array[Array[(Long, T)]] = {

        val gt = ds.GetGeoTransform
        val xSize = ds.getRasterXSize
        val ySize = ds.getRasterYSize
        val nPix = xSize * ySize
        val bands = ds.getRasterCount

        val bandBuf = new Array[Double](nPix)
        val maskBuf = new Array[Byte](nPix)

        (1 to bands).iterator.map { bi =>
            val b = ds.GetRasterBand(bi)
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
                        val cell = cellPixel(gt, x, y, resolution)
                        val buf = acc.getOrElseUpdate(cell, new mutable.ArrayBuffer)
                        buf += bandBuf(idx)
                    }
                    idx += 1; x += 1
                }
                y += 1
            }

            val out = new Array[(Long, T)](acc.size)
            var j = 0
            acc.foreach { case (cell, buf) => out(j) = (cell, fAgg(buf)); j += 1 }
            out
        }.toArray
    }

    def eval[T](
        row: InternalRow,
        resolution: Int,
        conf: UTF8String,
        rdt: DataType,
        execute: (Dataset, Int) => Array[Array[(Long, T)]]
    ): ArrayData = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val result = execute(ds, resolution)
        RasterDriver.releaseDataset(ds)
        ArrayData.toArrayData(
          result.map(band =>
              ArrayData.toArrayData(
                band.map { case (cellId, measure) => InternalRow.fromSeq(Seq(cellId, measure)) }
              )
          )
        )
    }

}
