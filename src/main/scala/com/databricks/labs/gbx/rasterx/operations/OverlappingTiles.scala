package com.databricks.labs.gbx.rasterx.operations

import org.gdal.gdal.Dataset

/** Generates overlapping tile windows and retiles a raster with a given overlap percentage. */
object OverlappingTiles {

    /** Returns (x, y, xSize, ySize) windows with step = tileSize - overlap. */
    def generateWindows(
        ds: Dataset,
        tileWidth: Int,
        tileHeight: Int,
        overlapPercentage: Int
    ): Seq[(Int, Int, Int, Int)] = {
        val (xSize, ySize) = (ds.getRasterXSize, ds.getRasterYSize)
        val overlapWidth = Math.ceil(tileWidth * overlapPercentage / 100.0).toInt
        val overlapHeight = Math.ceil(tileHeight * overlapPercentage / 100.0).toInt
        for {
            x <- 0 until xSize by (tileWidth - overlapWidth)
            y <- 0 until ySize by (tileHeight - overlapHeight)
        } yield {
            val xOffset = Math.min(tileWidth, xSize - x)
            val yOffset = Math.min(tileHeight, ySize - y)
            (x, y, xOffset, yOffset)
        }
    }

    /** Iterator of (Dataset, metadata) over overlapping windows. Overlap is a percentage of tile size. Caller must release; iterator is AutoCloseable via ReTile. */
    def reTileIter(
        ds: Dataset,
        options: Map[String, String],
        tileWidth: Int,
        tileHeight: Int,
        overlapPercentage: Int
    ): Iterator[(Dataset, Map[String, String])] = {
        val windows = generateWindows(ds, tileWidth, tileHeight, overlapPercentage)
        ReTile.reTileIter(ds, options, windows)
    }

}
