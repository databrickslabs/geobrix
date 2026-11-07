package com.databricks.labs.gbx.rasterx.operations

/**
  * Exact Euclidean distance transform (Felzenszwalb-Huttenlocher): for every cell, the distance to the nearest
  * source cell, with a separate cell spacing along each axis.
  *
  * `gdal.ComputeProximity` is an approximation (a two-pass chamfer-like sweep, off by up to ~6%); this is the
  * exact answer, the one the lightweight tier gets from `scipy.ndimage.distance_transform_edt`. The transform is
  * separable: a 1-D squared-distance transform along each row, then along each column.
  */
object ExactDistance {

    /** Stands for "no source cell yet": a squared distance no real raster reaches. */
    private val Unreached: Double = 1e30

    /**
      * Squared distances to the nearest source cell, row-major, `width * height`. `fillRow(y, flags)` sets one
      * row's flags (`true` = source cell). With no source cell at all every
      * distance is [[Double.PositiveInfinity]].
      *
      * @param xSpacing ground size of a cell along a row (x), greater than 0
      * @param ySpacing ground size of a cell along a column (y), greater than 0
      */
    def squaredDistances(
        width: Int, height: Int, xSpacing: Double, ySpacing: Double, fillRow: (Int, Array[Boolean]) => Unit
    ): Array[Double] = {
        require(xSpacing > 0.0 && ySpacing > 0.0, s"ExactDistance: cell spacing must be > 0; got $xSpacing x $ySpacing")
        val n = math.max(width, height)
        val out = new Array[Double](width * height)
        val flags = new Array[Boolean](width)
        val f = new Array[Double](n)
        val d = new Array[Double](n)
        val v = new Array[Int](n)
        val z = new Array[Double](n)

        // pass 1: along each row, the squared distance to the nearest source cell of that row
        var y = 0
        while (y < height) {
            fillRow(y, flags)
            var x = 0
            while (x < width) { f(x) = if (flags(x)) 0.0 else Unreached; x += 1 }
            transform1d(f, width, xSpacing, d, v, z)
            System.arraycopy(d, 0, out, y * width, width)
            y += 1
        }
        // pass 2: along each column, the minimum over rows of (row-pass value + the vertical offset squared)
        var x = 0
        while (x < width) {
            var r = 0
            while (r < height) { f(r) = out(r * width + x); r += 1 }
            transform1d(f, height, ySpacing, d, v, z)
            r = 0
            while (r < height) { out(r * width + x) = if (d(r) >= Unreached) Double.PositiveInfinity else d(r); r += 1 }
            x += 1
        }
        out
    }

    /** `d(q) = min over p of f(p) + (s * (q - p))^2` for `q` in `[0, n)`; an `f` of [[Unreached]] or more is skipped. */
    private def transform1d(f: Array[Double], n: Int, s: Double, d: Array[Double], v: Array[Int], z: Array[Double]): Unit = {
        var k = -1
        var q = 0
        while (q < n) {
            if (f(q) < Unreached) {
                var placed = false
                while (!placed) {
                    if (k < 0) {
                        k = 0; v(0) = q; z(0) = Double.NegativeInfinity; placed = true
                    } else {
                        val p = v(k)
                        val qs = q * s
                        val ps = p * s
                        val at = ((f(q) + qs * qs) - (f(p) + ps * ps)) / (2.0 * s * (q - p))
                        if (at <= z(k)) k -= 1
                        else { k += 1; v(k) = q; z(k) = at; placed = true }
                    }
                }
            }
            q += 1
        }
        if (k < 0) java.util.Arrays.fill(d, 0, n, Unreached)
        else {
            var j = 0
            q = 0
            while (q < n) {
                val pos = q * s
                while (j < k && z(j + 1) < pos) j += 1
                val off = (q - v(j)) * s
                d(q) = f(v(j)) + off * off
                q += 1
            }
        }
    }
}
