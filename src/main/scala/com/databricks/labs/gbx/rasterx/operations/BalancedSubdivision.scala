package com.databricks.labs.gbx.rasterx.operations

import org.gdal.gdal.Dataset

/** Computes balanced tile dimensions (power-of-4 splits) and splits a raster into tiles via ReTile. */
object BalancedSubdivision {

    /**
      * Gets the tile size for a raster. The tile size is determined by the
      * number of splits. The tile size is always a power of 4. This is a
      * heuristic method only due to compressions and other factors.
      * @note
      *   Power of 2 is used to split the raster in each step but the number of
      *   splits is always a power of 4.
      *
      * @param destMiB
      *   The desired max size in MBs.
      * @return
      *   The tile size.
      */
    def getTileSize(ds: Dataset, destMiB: Int): (Int, Int) = {
        val x = ds.getRasterXSize
        val y = ds.getRasterYSize
        val sizeBytes = RasterAccessors.memSize(ds).toLong
        val limit = destMiB.toLong * 1024 * 1024
        // k = number of quad-split rounds; splits = 4^k; nx=ny=2^k
        var k = 0
        while (k < 9 && (sizeBytes >> (2 * k)) > limit && (1 << (2 * (k + 1))) <= 512) k += 1
        val nx = 1 << k
        val ny = 1 << k
        val tileX = (x + nx - 1) / nx // ceil-div
        val tileY = (y + ny - 1) / ny
        (tileX, tileY)
    }

    /** Seq of (Dataset, metadata) tiles; split count is power-of-4 heuristic from sizeInMb. Caller must release each Dataset. */
    def splitRaster(
        ds: Dataset,
        options: Map[String, String],
        sizeInMb: Int
    ): Seq[(Dataset, Map[String, String])] = splitRasterIter(ds, options, sizeInMb).toSeq

    /** Iterator of (Dataset, metadata) tiles using ReTile with computed tile dimensions. */
    def splitRasterIter(
        ds: Dataset,
        options: Map[String, String],
        sizeInMb: Int
    ): Iterator[(Dataset, Map[String, String])] = {
        val (tileX, tileY) = getTileSize(ds, sizeInMb)
        ReTile.reTileIter(ds, options, tileX, tileY)
    }

}
