package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import org.gdal.gdal.{Dataset, gdal}

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

    /** Extracts one window as a new Dataset; returns null if the tile is empty. Caller must release if non-null. */
    def getTile(
        ds: Dataset,
        options: Map[String, String],
        xStart: Int,
        yStart: Int,
        xOffset: Int,
        yOffset: Int
    ): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val driver = ds.GetDriver
        val extension = GDAL.getExtension(driver.getShortName)

        val rasterPath = s"/vsimem/retile_$uuid.$extension"

        val result = GDALTranslate.executeTranslate(
          rasterPath,
          ds,
          command = s"gdal_translate -srcwin $xStart $yStart $xOffset $yOffset",
          options
        )

        val isEmpty = RasterAccessors.isEmpty(result._1)

        if (isEmpty) {
            result._1.delete()
            gdal.Unlink(rasterPath)
            null
        } else {
            (result._1, result._2)
        }
    }

    /** Seq of (Dataset, metadata) tiles; empty tiles discarded. Caller must release each Dataset. */
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

            /** Fetches the next tile into nextTile or closes and nulls it when exhausted. */
            private def advance(): Unit = {
                fetched = true
                nextTile = null
                while (i < windows.length && nextTile == null) {
                    val (xs, ys, xo, yo) = windows(i)
                    i += 1
                    nextTile = getTile(_ds, options, xs, ys, xo, yo) // returns null if empty
                }
                if (i >= windows.length && nextTile == null) close()
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
