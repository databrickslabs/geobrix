package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.gridx.grid.H3
import com.databricks.labs.gbx.rasterx.gdal.GDAL
import org.gdal.gdal.Dataset

/** Tessellates a raster into H3 cells: clips by cell geometry and yields (cellId, Dataset, metadata) per cell. */
object RasterTessellate {

    /** Clips ds to the H3 cell geometry and returns (cellId, clipped Dataset, metadata); returns null if empty. */
    def getTile(ds: Dataset, options: Map[String, String], cell: Long): (Long, Dataset, Map[String, String]) = {
        val cellGeom = H3.cellIdToGeometry(cell)
        val (resDs, resMtd) = ClipToGeom.clip(ds, options, cellGeom, GDAL.WSG84)
        if (RasterAccessors.isEmpty(resDs)) return null
        resDs.SetMetadataItem("RASTERX_CELL_ID", cell.toString)
        resDs.FlushCache()
        (cell, resDs, resMtd)
    }

    /** Iterator of (cellId, Dataset, metadata) per H3 cell overlapping the raster bbox at resolution. Caller must release each Dataset; iterator is AutoCloseable. */
    def tessellateH3Iter(
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
            private val _cells = cells
            private var cc = 0
            private var nextTile: (Long, Dataset, Map[String, String]) = _

            /** Fetches the next (cell, Dataset, metadata) into nextTile or closes when exhausted. */
            private def advance(): Unit = {
                fetched = true
                nextTile = null
                while (cc < _cells.length && nextTile == null) {
                    val cell = _cells(cc)
                    nextTile = getTile(_ds, options, cell)
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

}
