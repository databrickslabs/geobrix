package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.Dataset
import org.gdal.osr.{CoordinateTransformation, SpatialReference}
import org.locationtech.jts.algorithm.Orientation
import org.locationtech.jts.geom.{Coordinate, Geometry}

/** Builds JTS polygon geometries for raster extents (full raster or a pixel window) in a target CRS. */
object BoundingBox {

    /** Returns the bounding box of the full raster in destSR (polygon, CCW). */
    def bbox(ds: Dataset, destSR: SpatialReference): Geometry = {
        val xSize = ds.GetRasterXSize
        val ySize = ds.GetRasterYSize
        windowBBox((0, 0, xSize, ySize), ds, destSR)
    }

    /** True when the footprint of `ds` must be transformed into `destSR`: the raster has a CRS and it
      * is not `destSR`. A CRS-less raster is taken as already in `destSR`. */
    private[operations] def reprojects(ds: Dataset, destSR: SpatialReference): Boolean = {
        val srcSR = ds.GetSpatialRef
        srcSR != null && srcSR.IsSame(destSR) != 1
    }

    /**
      * Footprint of the full raster in destSR (polygon, CCW): the pixel-grid rectangle
      * (`gt0 + x*gt1`, `gt3 + y*gt5`) for x in 0..width, y in 0..height. The rotation terms gt2/gt4 are
      * ignored, as in [[windowBBox]].
      *
      * When the footprint is transformed ([[reprojects]]), each of the four edges is walked at
      * `pointsPerEdge` evenly spaced points, corners included, before the transform, so the ring follows
      * the curved image of the edge (e.g. a UTM edge in lon/lat). The 4-corner chord of [[bbox]] cuts
      * inside that curve and loses real edge pixels. Without a transform the edges are straight and the
      * four corners are the footprint (a CRS-less raster is taken as already in destSR). The default of
      * 21 points is the lightweight tier's (rasterio's `transform_bounds` default).
      *
      * Throws IllegalArgumentException when a point cannot be transformed (a non-finite coordinate).
      */
    def densifiedBBox(ds: Dataset, destSR: SpatialReference, pointsPerEdge: Int = 21): Geometry = {
        require(pointsPerEdge >= 2, s"a footprint edge needs at least its two corners; got $pointsPerEdge points")
        val gt = ds.GetGeoTransform
        val (w, h) = (ds.GetRasterXSize.toDouble, ds.GetRasterYSize.toDouble)
        val transform = reprojects(ds, destSR)
        // Walk the edges corner to corner (top, right, bottom, left); each edge's last point is the next
        // edge's first, and the ring closes on its first point.
        val n = if (transform) pointsPerEdge - 1 else 1
        val frac = (0 until n).map(_.toDouble / n)
        val edges = frac.map(f => (f * w, 0.0)) ++ frac.map(f => (w, f * h)) ++
            frac.map(f => ((1 - f) * w, h)) ++ frac.map(f => (0.0, (1 - f) * h))
        val xys = (edges :+ edges.head).map { case (col, row) => (gt(0) + col * gt(1), gt(3) + row * gt(5)) }.toArray
        val points =
            if (!transform) xys
            else {
                val srcSR = ds.GetSpatialRef
                val tf = new CoordinateTransformation(srcSR, destSR)
                val out =
                    try xys.map { case (x, y) => val p = tf.TransformPoint(x, y); (p(0), p(1)) }
                    finally tf.delete()
                if (!out.forall { case (x, y) => !x.isNaN && !x.isInfinite && !y.isNaN && !y.isInfinite }) {
                    throw new IllegalArgumentException(
                      s"the raster corners cannot be transformed from ${SpatialRefOps.crsToCanonical(srcSR)} " +
                        s"to ${SpatialRefOps.crsToCanonical(destSR)}"
                    )
                }
                out
            }
        val isCCW = Orientation.isCCW(points.map { case (x, y) => new Coordinate(x, y) })
        val footprint = JTS.polygonFromXYs(if (isCCW) points else points.reverse)
        footprint.setSRID(SpatialRefOps.getEPSGCode(destSR))
        footprint
    }

    /** Returns the bounding box of the pixel window (minX, minY, maxX, maxY) in destSR. */
    def windowBBox(window: (Int, Int, Int, Int), ds: Dataset, destSR: SpatialReference): Geometry = {
        val gt = ds.GetGeoTransform
        val srcSR = ds.GetSpatialRef

        val tf = new CoordinateTransformation(srcSR, destSR)

        val p1 = tf.TransformPoint(gt(0) + window._1 * gt(1), gt(3) + window._2 * gt(5))
        val p2 = tf.TransformPoint(gt(0) + window._3 * gt(1), gt(3) + window._2 * gt(5))
        val p3 = tf.TransformPoint(gt(0) + window._3 * gt(1), gt(3) + window._4 * gt(5))
        val p4 = tf.TransformPoint(gt(0) + window._1 * gt(1), gt(3) + window._4 * gt(5))
        val p5 = p1 // Closing the polygon

        val points = Array(p1, p2, p3, p4, p5)
        val isCCW = Orientation.isCCW(points.map(p => new Coordinate(p(0), p(1))))
        val pointsOrd = if (isCCW) points else points.reverse
        val bbox = JTS.polygonFromXYs(pointsOrd.map(p => (p(0), p(1))))

        val dstEPSGCode = SpatialRefOps.getEPSGCode(destSR)
        bbox.setSRID(dstEPSGCode)

        if (destSR.IsSame(GDAL.WSG84) == 1) {
            // Make sure is safe wrt AntiMeridian
            bbox // TODO: Handle AntiMeridian crossing if necessary
        } else {
            bbox
        }
    }

}
