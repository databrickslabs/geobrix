package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.gridx.grid.{H3, Quadbin}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.Dataset
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.CoordinateTransformation
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Tessellates a raster into H3 cells: clips by cell geometry and yields (cellId, Dataset, metadata) per cell. */
object RasterTessellate {

    /** Supported tessellation modes. `covering` (default) keeps every cell whose hexagon overlaps the
      * raster bbox (chips may share pixels). `centroid` single-assigns each valid pixel to the one cell
      * whose hexagon contains its centroid (chips partition the valid pixels). */
    val Modes: Set[String] = Set("covering", "centroid")

    /**
      * Clips ds to the H3 cell geometry and returns (cellId, clipped Dataset, metadata); returns null if the
      * cell hexagon does NOT geometrically overlap the raster bbox.
      *
      * The covering set is defined geometrically: keep the cell iff its H3 hexagon (WGS84, same CRS as `bbox`)
      * intersects the raster bbox. This replaces an earlier nodata-mask keep-test (`RasterAccessors.isEmpty`
      * on the bbox-snapped warp), which over-included a fringe of cells whose hexagons sit just outside the
      * raster (zero geometric overlap). Matches the light tier's `contain='overlap'` covering set.
      */
    def getTile(
        ds: Dataset,
        options: Map[String, String],
        cell: Long,
        bbox: Geometry
    ): (Long, Dataset, Map[String, String]) = {
        val cellGeom = H3.cellIdToGeometry(cell)
        if (!cellGeom.intersects(bbox)) return null
        val (resDs, resMtd) = ClipToGeom.clip(ds, options, cellGeom, GDAL.WSG84)
        if (resDs == null) return null
        resDs.SetMetadataItem("RASTERX_CELL_ID", cell.toString)
        resDs.FlushCache()
        (cell, resDs, resMtd)
    }

    /**
      * Iterator of (cellId, Dataset, metadata) per emitted H3 cell at resolution. Caller must release each
      * Dataset; iterator is AutoCloseable.
      *
      *  - `covering` (default): one chip per cell whose hexagon overlaps the raster bbox (chips may overlap).
      *  - `centroid`: pixel-centroid single-assignment partition — each valid source pixel is assigned to the
      *    one cell whose hexagon contains its centroid (same per-pixel rule as `rst_h3_rastertogrid*`); each
      *    cell's chip holds only its assigned pixels (the rest nodata), so every valid pixel is in exactly one chip.
      */
    def tessellateH3Iter(
        ds: Dataset,
        options: Map[String, String],
        resolution: Int,
        mode: String = "covering"
    ): Iterator[(Long, Dataset, Map[String, String])] = {
        require(Modes.contains(mode), s"gbx_rst_h3_tessellate mode must be one of ${Modes.mkString(", ")}; got '$mode'")
        if (mode == "centroid") tessellateH3CentroidIter(ds, options, resolution)
        else tessellateH3CoveringIter(ds, options, resolution)
    }

    /** Covering tessellation: see [[tessellateH3Iter]]. */
    private def tessellateH3CoveringIter(
        ds: Dataset,
        options: Map[String, String],
        resolution: Int
    ): Iterator[(Long, Dataset, Map[String, String])] = {
        val bbox = BoundingBox.bbox(ds, GDAL.WSG84)
        val bufR = H3.getBufferRadius(bbox, resolution)
        val cells = H3.polyfill(bbox.buffer(bufR), resolution)

        new Iterator[(Long, Dataset, Map[String, String])] with AutoCloseable {
            private var closed = false
            private var fetched = false
            private var _ds = ds
            private val _bbox = bbox
            private val _cells = cells
            private var cc = 0
            private var nextTile: (Long, Dataset, Map[String, String]) = _

            /** Fetches the next (cell, Dataset, metadata) into nextTile or closes when exhausted. */
            private def advance(): Unit = {
                fetched = true
                nextTile = null
                while (cc < _cells.length && nextTile == null) {
                    val cell = _cells(cc)
                    nextTile = getTile(_ds, options, cell, _bbox)
                    cc += 1
                }
                if (cc >= _cells.length && nextTile == null) close()
            }

            /** Overrides Iterator.hasNext: true until advance() exhausts cells or close() called. */
            override def hasNext: Boolean = {
                if (!fetched && !closed) advance()
                !closed && nextTile != null
            }

            /** Overrides Iterator.next: returns (cellId, Dataset, metadata); caller must release Dataset. */
            override def next(): (Long, Dataset, Map[String, String]) = {
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

    /**
      * Centroid (single-assignment) tessellation: see [[tessellateH3Iter]].
      *
      * Per-pixel rule mirrors [[com.databricks.labs.gbx.rasterx.expressions.grid.RST_H3_RasterToGrid.cellPixel]]
      * exactly: the pixel centroid is `(gt0 + (x+0.5)*gt1 + (y+0.5)*gt2, gt3 + (x+0.5)*gt4 + (y+0.5)*gt5)`, then
      * `H3.pointToCellID(lon, lat, resolution)`. Note `pointToCellID` takes (lon, lat) (it calls `geoToH3(lat, lon)`),
      * so the X (easting/lon) coordinate is the first arg — matching RasterToGrid. If the raster CRS is not 4326 the
      * pixel centroid is reprojected to 4326 first (RasterToGrid assumes a 4326 raster and skips this; we are general).
      *
      * Each valid pixel is assigned to exactly one cell, so the emitted chips partition the valid pixels.
      */
    private def tessellateH3CentroidIter(
        ds: Dataset,
        options: Map[String, String],
        resolution: Int
    ): Iterator[(Long, Dataset, Map[String, String])] = {
        val xSize = ds.getRasterXSize
        val ySize = ds.getRasterYSize
        val nPix = xSize * ySize
        val bandCount = ds.getRasterCount
        val dtype = ds.GetRasterBand(1).getDataType
        val gt = ds.GetGeoTransform

        // Reproject pixel centroids to 4326 only when the raster CRS differs; null transform => use coords as-is.
        val srcSR = ds.GetSpatialRef
        val needReproject = srcSR != null && srcSR.IsSame(GDAL.WSG84) != 1
        val tf: CoordinateTransformation = if (needReproject) new CoordinateTransformation(srcSR, GDAL.WSG84) else null

        // Read every band's values + mask once; assign each valid pixel (by flat index) to its cell.
        val bandVals = new Array[Array[Double]](bandCount)
        val bandMask = new Array[Array[Byte]](bandCount)
        val bandNoData = new Array[Double](bandCount)
        var bi = 0
        while (bi < bandCount) {
            val band = ds.GetRasterBand(bi + 1)
            val vals = new Array[Double](nPix)
            val mask = new Array[Byte](nPix)
            band.ReadRaster(0, 0, xSize, ySize, vals)
            band.GetMaskBand().ReadRaster(0, 0, xSize, ySize, mask)
            bandVals(bi) = vals
            bandMask(bi) = mask
            val nd = new Array[java.lang.Double](1)
            band.GetNoDataValue(nd)
            // Need a concrete nodata to blank unassigned pixels; if the band has none, synthesize a sentinel.
            bandNoData(bi) = if (nd(0) != null) nd(0).doubleValue() else sentinelNoData(dtype)
            bi += 1
        }

        // cell -> set of flat pixel indices that fall in it (union across bands so every valid pixel is placed once).
        val cellPixels = new mutable.LongMap[mutable.ArrayBuffer[Int]]()
        var y = 0
        var idx = 0
        while (y < ySize) {
            var x = 0
            while (x < xSize) {
                var anyValid = false
                var b = 0
                while (b < bandCount && !anyValid) { if (bandMask(b)(idx) != 0) anyValid = true; b += 1 }
                if (anyValid) {
                    val xOff = 0.5 + x
                    val yOff = 0.5 + y
                    val xGeo = gt(0) + xOff * gt(1) + yOff * gt(2)
                    val yGeo = gt(3) + xOff * gt(4) + yOff * gt(5)
                    val (lon, lat) = if (tf != null) {
                        val p = tf.TransformPoint(xGeo, yGeo)
                        (p(0), p(1))
                    } else (xGeo, yGeo)
                    val cell = H3.pointToCellID(lon, lat, resolution)
                    cellPixels.getOrElseUpdate(cell, new mutable.ArrayBuffer[Int]) += idx
                }
                idx += 1
                x += 1
            }
            y += 1
        }

        val cellIter = cellPixels.iterator

        new Iterator[(Long, Dataset, Map[String, String])] with AutoCloseable {
            private var closed = false

            override def hasNext: Boolean = !closed && cellIter.hasNext

            override def next(): (Long, Dataset, Map[String, String]) = {
                val (cell, pixIdx) = cellIter.next()
                val tile = buildCentroidChip(ds, options, cell, pixIdx, xSize, ySize, bandCount, dtype, gt, bandVals, bandNoData)
                if (!cellIter.hasNext) close()
                tile
            }

            override def close(): Unit = { closed = true }
        }
    }

    /** Builds one full-extent chip holding only `cell`'s assigned pixels (the rest nodata) for [[tessellateH3CentroidIter]]. */
    private def buildCentroidChip(
        ds: Dataset,
        options: Map[String, String],
        cell: Long,
        pixIdx: mutable.ArrayBuffer[Int],
        xSize: Int,
        ySize: Int,
        bandCount: Int,
        dtype: Int,
        gt: Array[Double],
        bandVals: Array[Array[Double]],
        bandNoData: Array[Double]
    ): (Long, Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val rasterPath = s"/vsimem/h3_centroid_${cell}_$uuid.tif"
        val drv = GDALManager.gtiffDriver()
        val out = drv.Create(rasterPath, xSize, ySize, bandCount, dtype)
        out.SetGeoTransform(gt)
        out.SetProjection(ds.GetProjection())

        val nPix = xSize * ySize
        var b = 0
        while (b < bandCount) {
            val nd = bandNoData(b)
            val src = bandVals(b)
            val buf = new Array[Double](nPix)
            java.util.Arrays.fill(buf, nd) // blank everything to nodata...
            var i = 0
            while (i < pixIdx.length) { val p = pixIdx(i); buf(p) = src(p); i += 1 } // ...then restore assigned pixels
            val db = out.GetRasterBand(b + 1)
            db.SetNoDataValue(nd)
            // Implicit Float64 buffer overload: GDAL converts the double[] to the band's native dtype on
            // write (mirrors RST_H3_RasterToGrid's ReadRaster(double[])). Passing the band dtype as the
            // buffer type with a double[] would misinterpret the bytes.
            db.WriteRaster(0, 0, xSize, ySize, buf)
            b += 1
        }
        out.SetMetadataItem("RASTERX_CELL_ID", cell.toString)
        out.FlushCache()

        val sourcePath = Option(ds.GetFileList())
            .flatMap(_.asScala.headOption.map(_.toString))
            .getOrElse("unknown source path")
        val meta = Map(
          "path" -> rasterPath,
          "parentPath" -> options.getOrElse("path", sourcePath),
          "driver" -> "GTiff",
          "format" -> "GTiff",
          "last_command" -> s"h3_centroid_tessellate cell=$cell",
          "last_error" -> "",
          "all_parents" -> s"$sourcePath;${options.getOrElse("all_parents", "")}",
          "size" -> "-1",
          "compression" -> options.getOrElse("compression", "DEFLATE"),
          "isZipped" -> "false",
          "isSubset" -> "false"
        )
        (cell, out, meta)
    }

    /** A nodata sentinel for bands lacking an explicit nodata, by data type (used only to blank unassigned pixels). */
    private def sentinelNoData(dtype: Int): Double = {
        // Float types: NaN is the natural sentinel. Integer types: 0 (chips for centroid mode set it as nodata
        // so the mask treats it as invalid; collisions with real 0-valued data are acceptable for blanking only
        // when no explicit nodata exists, which is rare for the rasters this path serves).
        if (dtype == gdalconstConstants.GDT_Float32 || dtype == gdalconstConstants.GDT_Float64) Double.NaN else 0.0
    }

    // ------------------------------------------------------------------------------------------------
    // Quadbin tessellation (parallel clone of the H3 path above; quadbin is 4326-native like H3, so no
    // reprojection/warp — the raster is assumed EPSG:4326 lon/lat, exactly as the H3 tessellate assumes).
    // Cell ids are Long end-to-end (no string format), enumerated/geometrised via `Quadbin`.
    // ------------------------------------------------------------------------------------------------

    /**
      * Clips ds to the quadbin cell geometry and returns (cellId, clipped Dataset, metadata); returns null if the
      * cell tile does NOT geometrically overlap the raster bbox. Clone of [[getTile]] for quadbin: the cell polygon
      * is built from `Quadbin.cellBbox` (EPSG:4326 lon/lat, same CRS as `bbox`) and the same intersect keep-test /
      * clip is applied.
      */
    def getQuadbinTile(
        ds: Dataset,
        options: Map[String, String],
        cell: Long,
        bbox: Geometry
    ): (Long, Dataset, Map[String, String]) = {
        val cellGeom = quadbinCellGeometry(cell)
        if (!cellGeom.intersects(bbox)) return null
        val (resDs, resMtd) = ClipToGeom.clip(ds, options, cellGeom, GDAL.WSG84)
        if (resDs == null) return null
        resDs.SetMetadataItem("RASTERX_CELL_ID", cell.toString)
        resDs.FlushCache()
        (cell, resDs, resMtd)
    }

    /** Quadbin cell -> JTS polygon (EPSG:4326, SRID 4326), built from `Quadbin.cellBbox` = (lonMin,latMin,lonMax,latMax). */
    private def quadbinCellGeometry(cell: Long): Geometry = {
        val (lonMin, latMin, lonMax, latMax) = Quadbin.cellBbox(cell)
        val geom = JTS.polygonFromXYs(
          Array((lonMin, latMin), (lonMax, latMin), (lonMax, latMax), (lonMin, latMax), (lonMin, latMin))
        )
        geom.setSRID(4326) // EPSG:4326, matching H3.cellIdToGeometry's crsID
        geom
    }

    /**
      * Iterator of (cellId, Dataset, metadata) per emitted quadbin cell at `resolution` (zoom z). Caller must release
      * each Dataset; iterator is AutoCloseable. Parallel to [[tessellateH3Iter]].
      *
      *  - `covering` (default): one chip per cell whose tile overlaps the raster bbox (chips may overlap).
      *  - `centroid`: pixel-centroid single-assignment partition — each valid source pixel is assigned to the one
      *    cell whose tile contains its centroid; each cell's chip holds only its assigned pixels (the rest nodata).
      */
    def tessellateQuadbinIter(
        ds: Dataset,
        options: Map[String, String],
        resolution: Int,
        mode: String = "covering"
    ): Iterator[(Long, Dataset, Map[String, String])] = {
        require(
          Modes.contains(mode),
          s"gbx_rst_quadbin_tessellate mode must be one of ${Modes.mkString(", ")}; got '$mode'"
        )
        if (mode == "centroid") tessellateQuadbinCentroidIter(ds, options, resolution)
        else tessellateQuadbinCoveringIter(ds, options, resolution)
    }

    /** Covering tessellation: see [[tessellateQuadbinIter]]. Clone of [[tessellateH3CoveringIter]]. */
    private def tessellateQuadbinCoveringIter(
        ds: Dataset,
        options: Map[String, String],
        resolution: Int
    ): Iterator[(Long, Dataset, Map[String, String])] = {
        val bbox = BoundingBox.bbox(ds, GDAL.WSG84)
        val env = bbox.getEnvelopeInternal
        val cells = Quadbin.polyfillBbox((env.getMinX, env.getMinY, env.getMaxX, env.getMaxY), resolution)

        new Iterator[(Long, Dataset, Map[String, String])] with AutoCloseable {
            private var closed = false
            private var fetched = false
            private var _ds = ds
            private val _bbox = bbox
            private val _cells = cells
            private var cc = 0
            private var nextTile: (Long, Dataset, Map[String, String]) = _

            /** Fetches the next (cell, Dataset, metadata) into nextTile or closes when exhausted. */
            private def advance(): Unit = {
                fetched = true
                nextTile = null
                while (cc < _cells.length && nextTile == null) {
                    val cell = _cells(cc)
                    nextTile = getQuadbinTile(_ds, options, cell, _bbox)
                    cc += 1
                }
                if (cc >= _cells.length && nextTile == null) close()
            }

            /** Overrides Iterator.hasNext: true until advance() exhausts cells or close() called. */
            override def hasNext: Boolean = {
                if (!fetched && !closed) advance()
                !closed && nextTile != null
            }

            /** Overrides Iterator.next: returns (cellId, Dataset, metadata); caller must release Dataset. */
            override def next(): (Long, Dataset, Map[String, String]) = {
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

    /**
      * Centroid (single-assignment) tessellation: see [[tessellateQuadbinIter]]. Clone of [[tessellateH3CentroidIter]],
      * substituting `Quadbin.pointToCell(lon, lat, z)` for the H3 point-to-cell rule. If the raster CRS is not 4326 the
      * pixel centroid is reprojected to 4326 first.
      */
    private def tessellateQuadbinCentroidIter(
        ds: Dataset,
        options: Map[String, String],
        resolution: Int
    ): Iterator[(Long, Dataset, Map[String, String])] = {
        val xSize = ds.getRasterXSize
        val ySize = ds.getRasterYSize
        val nPix = xSize * ySize
        val bandCount = ds.getRasterCount
        val dtype = ds.GetRasterBand(1).getDataType
        val gt = ds.GetGeoTransform

        // Reproject pixel centroids to 4326 only when the raster CRS differs; null transform => use coords as-is.
        val srcSR = ds.GetSpatialRef
        val needReproject = srcSR != null && srcSR.IsSame(GDAL.WSG84) != 1
        val tf: CoordinateTransformation = if (needReproject) new CoordinateTransformation(srcSR, GDAL.WSG84) else null

        // Read every band's values + mask once; assign each valid pixel (by flat index) to its cell.
        val bandVals = new Array[Array[Double]](bandCount)
        val bandMask = new Array[Array[Byte]](bandCount)
        val bandNoData = new Array[Double](bandCount)
        var bi = 0
        while (bi < bandCount) {
            val band = ds.GetRasterBand(bi + 1)
            val vals = new Array[Double](nPix)
            val mask = new Array[Byte](nPix)
            band.ReadRaster(0, 0, xSize, ySize, vals)
            band.GetMaskBand().ReadRaster(0, 0, xSize, ySize, mask)
            bandVals(bi) = vals
            bandMask(bi) = mask
            val nd = new Array[java.lang.Double](1)
            band.GetNoDataValue(nd)
            // Need a concrete nodata to blank unassigned pixels; if the band has none, synthesize a sentinel.
            bandNoData(bi) = if (nd(0) != null) nd(0).doubleValue() else sentinelNoData(dtype)
            bi += 1
        }

        // cell -> set of flat pixel indices that fall in it (union across bands so every valid pixel is placed once).
        val cellPixels = new mutable.LongMap[mutable.ArrayBuffer[Int]]()
        var y = 0
        var idx = 0
        while (y < ySize) {
            var x = 0
            while (x < xSize) {
                var anyValid = false
                var b = 0
                while (b < bandCount && !anyValid) { if (bandMask(b)(idx) != 0) anyValid = true; b += 1 }
                if (anyValid) {
                    val xOff = 0.5 + x
                    val yOff = 0.5 + y
                    val xGeo = gt(0) + xOff * gt(1) + yOff * gt(2)
                    val yGeo = gt(3) + xOff * gt(4) + yOff * gt(5)
                    val (lon, lat) = if (tf != null) {
                        val p = tf.TransformPoint(xGeo, yGeo)
                        (p(0), p(1))
                    } else (xGeo, yGeo)
                    val cell = Quadbin.pointToCell(lon, lat, resolution)
                    cellPixels.getOrElseUpdate(cell, new mutable.ArrayBuffer[Int]) += idx
                }
                idx += 1
                x += 1
            }
            y += 1
        }

        val cellIter = cellPixels.iterator

        new Iterator[(Long, Dataset, Map[String, String])] with AutoCloseable {
            private var closed = false

            override def hasNext: Boolean = !closed && cellIter.hasNext

            override def next(): (Long, Dataset, Map[String, String]) = {
                val (cell, pixIdx) = cellIter.next()
                val tile =
                    buildQuadbinCentroidChip(ds, options, cell, pixIdx, xSize, ySize, bandCount, dtype, gt, bandVals, bandNoData)
                if (!cellIter.hasNext) close()
                tile
            }

            override def close(): Unit = { closed = true }
        }
    }

    /** Builds one full-extent chip holding only `cell`'s assigned pixels (the rest nodata) for [[tessellateQuadbinCentroidIter]]. Clone of [[buildCentroidChip]]. */
    private def buildQuadbinCentroidChip(
        ds: Dataset,
        options: Map[String, String],
        cell: Long,
        pixIdx: mutable.ArrayBuffer[Int],
        xSize: Int,
        ySize: Int,
        bandCount: Int,
        dtype: Int,
        gt: Array[Double],
        bandVals: Array[Array[Double]],
        bandNoData: Array[Double]
    ): (Long, Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val rasterPath = s"/vsimem/quadbin_centroid_${cell}_$uuid.tif"
        val drv = GDALManager.gtiffDriver()
        val out = drv.Create(rasterPath, xSize, ySize, bandCount, dtype)
        out.SetGeoTransform(gt)
        out.SetProjection(ds.GetProjection())

        val nPix = xSize * ySize
        var b = 0
        while (b < bandCount) {
            val nd = bandNoData(b)
            val src = bandVals(b)
            val buf = new Array[Double](nPix)
            java.util.Arrays.fill(buf, nd) // blank everything to nodata...
            var i = 0
            while (i < pixIdx.length) { val p = pixIdx(i); buf(p) = src(p); i += 1 } // ...then restore assigned pixels
            val db = out.GetRasterBand(b + 1)
            db.SetNoDataValue(nd)
            db.WriteRaster(0, 0, xSize, ySize, buf)
            b += 1
        }
        out.SetMetadataItem("RASTERX_CELL_ID", cell.toString)
        out.FlushCache()

        val sourcePath = Option(ds.GetFileList())
            .flatMap(_.asScala.headOption.map(_.toString))
            .getOrElse("unknown source path")
        val meta = Map(
          "path" -> rasterPath,
          "parentPath" -> options.getOrElse("path", sourcePath),
          "driver" -> "GTiff",
          "format" -> "GTiff",
          "last_command" -> s"quadbin_centroid_tessellate cell=$cell",
          "last_error" -> "",
          "all_parents" -> s"$sourcePath;${options.getOrElse("all_parents", "")}",
          "size" -> "-1",
          "compression" -> options.getOrElse("compression", "DEFLATE"),
          "isZipped" -> "false",
          "isSubset" -> "false"
        )
        (cell, out, meta)
    }

}
