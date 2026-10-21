package com.databricks.labs.gbx.rasterx.operations

import org.gdal.gdal.Dataset

/** Splits a raster into non-overlapping tiles (windows); supports retile and getTile. */
object ReTile {

    /** Returns a sequence of (xMin, yMin, xSize, ySize) windows covering the dataset. */
    def generateWindows(
        ds: Dataset,
        tileWidth: Int,
        tileHeight: Int
    ): Seq[(Int, Int, Int, Int)] = {
        val xR = ds.getRasterXSize
        val yR = ds.getRasterYSize
        val xTiles = (xR + tileWidth  - 1) / tileWidth
        val yTiles = (yR + tileHeight - 1) / tileHeight
        for {
            y <- 0 until yTiles
            x <- 0 until xTiles
        } yield {
            val xMin = x * tileWidth
            val yMin = y * tileHeight
            val xOff = math.min(tileWidth, xR - xMin)
            val yOff = math.min(tileHeight, yR - yMin)
            (xMin, yMin, xOff, yOff)
        }
    }

    /**
      * Extracts one window as a new Dataset. Every window is returned, including a window whose
      * pixels are all NoData (so the tile count is `ceil(W/tw) * ceil(H/th)`). Caller must release.
      */
    def getTile(
        ds: Dataset,
        options: Map[String, String],
        xStart: Int,
        yStart: Int,
        xOffset: Int,
        yOffset: Int
    ): (Dataset, Map[String, String]) = {
        WindowedExtract.extract(ds, options, xStart, yStart, xOffset, yOffset)
    }

    /** Seq of (Dataset, metadata) tiles, one per window (all-NoData windows included). Caller must release each Dataset. */
    def reTile(
        ds: Dataset,
        options: Map[String, String],
        tileWidth: Int,
        tileHeight: Int
    ): Seq[(Dataset, Map[String, String])] = reTileIter(ds, options, tileWidth, tileHeight).toSeq

    /** Iterator of (Dataset, metadata) tiles for the given tile dimensions; closes ds when exhausted. */
    def reTileIter(
        ds: Dataset,
        options: Map[String, String],
        tileWidth: Int,
        tileHeight: Int
    ): Iterator[(Dataset, Map[String, String])] = {
        val windows = generateWindows(ds, tileWidth, tileHeight)
        reTileIter(ds, options, windows)
    }

    /** Iterator over the given windows; AutoCloseable and releases ds when closed or exhausted. */
    def reTileIter(
        ds: Dataset,
        options: Map[String, String],
        windows: Seq[(Int, Int, Int, Int)]
    ): Iterator[(Dataset, Map[String, String])] with AutoCloseable = {
        new Iterator[(Dataset, Map[String, String])] with AutoCloseable {
            private var _ds = ds
            private var i = 0
            private var fetched = false
            private var closed = false
            private var nextTile: (Dataset, Map[String, String]) = _

            /** Fetches the next window into nextTile, or closes and nulls it when the windows are exhausted. */
            private def advance(): Unit = {
                fetched = true
                nextTile = null
                if (i < windows.length) {
                    val (xs, ys, xo, yo) = windows(i)
                    i += 1
                    nextTile = getTile(_ds, options, xs, ys, xo, yo)
                }
                if (nextTile == null) close()
            }

            /** Overrides Iterator.hasNext: true until advance() exhausts windows or close() called. */
            override def hasNext: Boolean = {
                if (!fetched && !closed) advance()
                !closed && nextTile != null
            }

            /** Overrides Iterator.next: returns next (Dataset, metadata) from advance(); caller must release Dataset. */
            override def next(): (Dataset, Map[String, String]) = {
                if (!fetched && !closed) advance()
                fetched = false
                nextTile
            }

            /** Overrides AutoCloseable.close: unlinks dataset and nulls reference; idempotent. */
            override def close(): Unit = {
                if (!closed) {
                    closed = true
                    RasterAccessors.unlink(_ds)
                    _ds = null
                }
            }

        }
    }

}
