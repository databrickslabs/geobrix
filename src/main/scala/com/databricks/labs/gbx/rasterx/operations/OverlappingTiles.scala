package com.databricks.labs.gbx.rasterx.operations

import org.gdal.gdal.Dataset

/** Generates overlapping tile windows and retiles a raster with a given overlap percentage. */
object OverlappingTiles {

    /** The window and overlap of `gbx_rst_tooverlappingtiles`, checked before any tile is read. A window below one
      * pixel or an overlap outside 0 to below 100 percent never advances through the raster: it is a mistake in the
      * call, so it raises with a Fix (as the lightweight level does) and is never the row's error tile. A NULL is
      * data, left to the row's error tile. */
    def requireArguments(fn: String, tileWidth: Any, tileHeight: Any, overlap: Any): Unit = {
        Seq("tile_width" -> tileWidth, "tile_height" -> tileHeight).foreach { case (name, v) =>
            if (v != null) {
                val n = v.asInstanceOf[Number].intValue
                require(n > 0, s"$fn: $name must be 1 or more pixels, got $n. Fix: pass a window of at least 1 pixel.")
            }
        }
        if (overlap != null) {
            val n = overlap.asInstanceOf[Number].intValue
            require(
              n >= 0 && n < 100,
              s"$fn: overlap must be from 0 to below 100 percent, got $n. Fix: pass the overlap as a percentage of the tile size."
            )
        }
    }

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
