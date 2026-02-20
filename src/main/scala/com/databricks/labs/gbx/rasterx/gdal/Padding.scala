package com.databricks.labs.gbx.rasterx.gdal

/**
  * Case class: describes which edges (left, right, top, bottom) have padding, e.g. for kernel operations.
  * Used by [[com.databricks.labs.gbx.rasterx.expressions.RST_Convolve]] and similar to strip padding
  * and adjust offset/size when reading convolved results.
  */
case class Padding(
    left: Boolean,
    right: Boolean,
    top: Boolean,
    bottom: Boolean
) {

    /** Strips padded rows/columns from a flat array (rowWidth, stride) and returns flat array. */
    def removePadding(array: Array[Double], rowWidth: Int, stride: Int): Array[Double] = {
        val l = if (left) 1 else 0
        val r = if (right) 1 else 0
        val t = if (top) 1 else 0
        val b = if (bottom) 1 else 0

        val yStart = t * stride * rowWidth
        val yEnd = array.length - b * stride * rowWidth

        val slices = for (i <- yStart until yEnd by rowWidth) yield {
            val xStart = i + l * stride
            val xEnd = i + rowWidth - r * stride
            array.slice(xStart, xEnd)
        }

        slices.flatten.toArray
    }

    /** Number of stride units to subtract from width when padding is present (0, 1, or 2). */
    def horizontalStrides: Int = {
        if (left && right) 2
        else if (left || right) 1
        else 0
    }

    /** Number of stride units to subtract from height when padding is present (0, 1, or 2). */
    def verticalStrides: Int = {
        if (top && bottom) 2
        else if (top || bottom) 1
        else 0
    }

    /** Returns (x, y) offset after removing left/top padding (adds stride if padded). */
    def newOffset(xOffset: Int, yOffset: Int, stride: Int): (Int, Int) = {
        val x = if (left) xOffset + stride else xOffset
        val y = if (top) yOffset + stride else yOffset
        (x, y)
    }

    /** Returns (width, height) after removing padded edges (uses horizontalStrides/verticalStrides). */
    def newSize(width: Int, height: Int, stride: Int): (Int, Int) = {
        val w = if (left && right) width - 2 * stride else if (left || right) width - stride else width
        val h = if (top && bottom) height - 2 * stride else if (top || bottom) height - stride else height
        (w, h)
    }

}

/** Companion: NoPadding singleton and (optionally) other presets. */
object Padding {

    /** Singleton: no padding on any edge; use when no kernel padding is applied. */
    val NoPadding: Padding = Padding(left = false, right = false, top = false, bottom = false)

}
