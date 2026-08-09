package com.databricks.labs.gbx.gridx.grid

/** CARTO quadbin v0 cell-math. Pure functions; no Spark / no GDAL dependency.
  *
  * Layout (64-bit Long) — matches the canonical
  * [[https://github.com/CartoDB/quadbin-py CARTO quadbin-py]] reference implementation:
  *   - bit 62 (HEADER): set to 1 (0x4000_0000_0000_0000)
  *   - bits 59..61: mode (= 0b001 for cells)
  *   - bits 52..58: resolution (z in [0..26])
  *   - bits 0..51 : Morton-interleaved (x, y) tile coordinates, FOOTER-padded
  *
  * Coordinates are EPSG:4326 lon/lat on input; encoded into web-mercator (z, x, y) tiles
  * internally. The grid is the standard XYZ "slippy map" tile grid (x increases east,
  * y increases south).
  */
object Quadbin extends Serializable {

    /** Header constant: bit 62 set. */
    private[gbx] val HEADER: Long = 0x4000000000000000L

    /** Mode = 1 (cell), at bits 59..61. */
    private[gbx] val MODE_BITS: Long = 1L << 59

    /** Trailing-bit mask for cell-payload: low 52 bits set. */
    private[gbx] val FOOTER: Long = 0xfffffffffffffL

    /** Latitude clamp for web-mercator. */
    private val LAT_MIN: Double = -85.05112878
    private val LAT_MAX: Double = 85.05112878

    /** Max supported resolution (CARTO v0 spec). */
    val MAX_RESOLUTION: Int = 26

    /** Bit-interleave masks. */
    private val B0: Long = 0x5555555555555555L
    private val B1: Long = 0x3333333333333333L
    private val B2: Long = 0x0f0f0f0f0f0f0f0fL
    private val B3: Long = 0x00ff00ff00ff00ffL
    private val B4: Long = 0x0000ffff0000ffffL

    /** Convert (lon, lat) at zoom z to the quadbin cell containing it. */
    def pointToCell(lon: Double, lat: Double, z: Int): Long = {
        require(z >= 0 && z <= MAX_RESOLUTION, s"quadbin resolution must be in [0, $MAX_RESOLUTION]; got $z")
        val (x, y) = lonLatToTile(lon, lat, z)
        tileToCell(z, x, y)
    }

    /** Convert (lon, lat) at zoom z to a (xTile, yTile) tuple.
      *
      * Latitude is intentionally clamped to the web-mercator limits ±85.05112878° and
      * longitude to ±180° — matching every standard slippy-map tiler. An out-of-range
      * coordinate is snapped to the nearest valid boundary tile rather than returning NULL
      * or raising an exception. This is deliberate, documented behavior, consistent with
      * the CARTO quadbin-py reference implementation.
      *
      * This is distinct from BNG and Custom grids, which return NULL for coordinates
      * outside their extent. Quadbin covers the whole web-mercator world; there is no
      * "outside" — only boundary clamping.
      */
    def lonLatToTile(lon: Double, lat: Double, z: Int): (Long, Long) = {
        val latClamped = math.max(LAT_MIN, math.min(LAT_MAX, lat))
        val lonClamped = math.max(-180.0, math.min(180.0, lon))
        val n: Long = if (z == 0) 1L else 1L << z
        val latRad = latClamped * math.Pi / 180.0
        var xTile = math.floor((lonClamped + 180.0) / 360.0 * n.toDouble).toLong
        var yTile = math.floor(
          (1.0 - math.log(math.tan(latRad) + 1.0 / math.cos(latRad)) / math.Pi) / 2.0 * n.toDouble
        ).toLong
        if (xTile < 0L) xTile = 0L
        if (xTile > n - 1L) xTile = n - 1L
        if (yTile < 0L) yTile = 0L
        if (yTile > n - 1L) yTile = n - 1L
        (xTile, yTile)
    }

    /** Pack (z, x, y) into the 64-bit quadbin Long (canonical CARTO v0 encoding). */
    def tileToCell(z: Int, x: Long, y: Long): Long = {
        require(z >= 0 && z <= MAX_RESOLUTION, s"quadbin resolution must be in [0, $MAX_RESOLUTION]; got $z")
        val n: Long = if (z == 0) 1L else 1L << z
        val xC = math.max(0L, math.min(n - 1L, x))
        val yC = math.max(0L, math.min(n - 1L, y))
        // Shift to 32-bit positions, then bit-interleave (x in even bits, y << 1 in odd bits).
        var xx = xC << (32 - z)
        var yy = yC << (32 - z)
        xx = (xx | (xx << 16)) & B4
        yy = (yy | (yy << 16)) & B4
        xx = (xx | (xx << 8))  & B3
        yy = (yy | (yy << 8))  & B3
        xx = (xx | (xx << 4))  & B2
        yy = (yy | (yy << 4))  & B2
        xx = (xx | (xx << 2))  & B1
        yy = (yy | (yy << 2))  & B1
        xx = (xx | (xx << 1))  & B0
        yy = (yy | (yy << 1))  & B0
        val interleaved = (xx | (yy << 1)) >>> 12
        // FOOTER >> (2*z) fills the unused trailing bits with 1s — matches CARTO encoding.
        HEADER | MODE_BITS | (z.toLong << 52) | interleaved | (FOOTER >>> (z * 2))
    }

    /** Alias matching plan API. */
    def encode(z: Int, x: Long, y: Long): Long = tileToCell(z, x, y)

    /** Extract resolution z from cell (bits 52..58). */
    def resolution(cell: Long): Int = ((cell >>> 52) & 0x1fL).toInt

    /** Extract (x, y) tile coords from cell. */
    def cellXY(cell: Long): (Long, Long) = {
        val z = resolution(cell)
        val q = (cell & FOOTER) << 12
        var x = q
        var y = q >>> 1
        x = x & B0; y = y & B0
        x = (x | (x >>> 1)) & B1
        y = (y | (y >>> 1)) & B1
        x = (x | (x >>> 2)) & B2
        y = (y | (y >>> 2)) & B2
        x = (x | (x >>> 4)) & B3
        y = (y | (y >>> 4)) & B3
        x = (x | (x >>> 8)) & B4
        y = (y | (y >>> 8)) & B4
        x = (x | (x >>> 16)) & 0xffffffffL
        y = (y | (y >>> 16)) & 0xffffffffL
        (x >>> (32 - z), y >>> (32 - z))
    }

    /** Bounding box of cell in EPSG:4326 lon/lat. Returns (lonMin, latMin, lonMax, latMax). */
    def cellBbox(cell: Long): (Double, Double, Double, Double) = {
        val z = resolution(cell)
        val (x, y) = cellXY(cell)
        val n: Double = math.pow(2.0, z.toDouble)
        val lonMin = x.toDouble / n * 360.0 - 180.0
        val lonMax = (x.toDouble + 1.0) / n * 360.0 - 180.0
        val latMax = tile2lat(y.toDouble, n)
        val latMin = tile2lat(y.toDouble + 1.0, n)
        (lonMin, latMin, lonMax, latMax)
    }

    private def tile2lat(yTile: Double, n: Double): Double = {
        val nRad = math.Pi - 2.0 * math.Pi * yTile / n
        math.atan(0.5 * (math.exp(nRad) - math.exp(-nRad))) * 180.0 / math.Pi
    }

    /** Centroid of cell in EPSG:4326 (lon, lat). */
    def cellCenter(cell: Long): (Double, Double) = {
        val (xmin, ymin, xmax, ymax) = cellBbox(cell)
        ((xmin + xmax) / 2.0, (ymin + ymax) / 2.0)
    }

    /** Chebyshev distance between two cells at the same resolution. */
    def cellDistance(a: Long, b: Long): Int = {
        require(resolution(a) == resolution(b), "quadbin_distance: cells must be at same resolution")
        val (ax, ay) = cellXY(a)
        val (bx, by) = cellXY(b)
        math.max(math.abs(ax - bx), math.abs(ay - by)).toInt
    }

    /** k-ring (Chebyshev distance ≤ k, inclusive) around `cell`. World-edge cells clip. */
    def kRing(cell: Long, k: Int): Array[Long] = {
        require(k >= 0, s"k must be >= 0; got $k")
        val z = resolution(cell)
        val n: Long = if (z == 0) 1L else 1L << z
        val (cx, cy) = cellXY(cell)
        val buf = scala.collection.mutable.ArrayBuffer.empty[Long]
        var dx = -k
        while (dx <= k) {
            var dy = -k
            while (dy <= k) {
                val nx = cx + dx
                val ny = cy + dy
                if (nx >= 0L && nx < n && ny >= 0L && ny < n) buf += tileToCell(z, nx, ny)
                dy += 1
            }
            dx += 1
        }
        buf.toArray
    }

    /** Polyfill an axis-aligned lon/lat bbox with cells at zoom `z` (cell-count guarded). */
    def polyfillBbox(bbox: (Double, Double, Double, Double), z: Int, maxCells: Int = 1_000_000): Array[Long] = {
        require(z >= 0 && z <= MAX_RESOLUTION, s"quadbin resolution must be in [0, $MAX_RESOLUTION]; got $z")
        val (lonMin, latMin, lonMax, latMax) = bbox
        val (x0, y0) = lonLatToTile(lonMin, latMax, z) // upper-left
        val (x1, y1) = lonLatToTile(lonMax, latMin, z) // lower-right
        val xLo = math.min(x0, x1)
        val xHi = math.max(x0, x1)
        val yLo = math.min(y0, y1)
        val yHi = math.max(y0, y1)
        val count = (xHi - xLo + 1L) * (yHi - yLo + 1L)
        require(count <= maxCells, s"polyfill would produce $count cells (max=$maxCells); use a lower zoom")
        val buf = scala.collection.mutable.ArrayBuffer.empty[Long]
        var x = xLo
        while (x <= xHi) {
            var y = yLo
            while (y <= yHi) {
                buf += tileToCell(z, x, y)
                y += 1
            }
            x += 1
        }
        buf.toArray
    }
}
