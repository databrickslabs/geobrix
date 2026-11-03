package com.databricks.labs.gbx.rasterx.operations

import org.locationtech.jts.geom.Geometry

/** Shared positive-area overlap test for raster→grid functions.
  *
  * Extracted from [[RasterTessellate]] so the same
  * keep-test applies in the `complete` coverage path without duplicating the rule.
  *
  * Accessible from all code within the `rasterx` package scope.
  */
private[rasterx] object GridOverlap {

    /** Returns true iff `cellGeom` has POSITIVE-AREA intersection with `bbox`.
      *
      * A shared edge or corner touch (1-D or 0-D intersection) does not qualify. On a
      * grid-aligned tile, fringe cells just outside the data boundary share only a 1-D
      * boundary with the raster: `intersects == true` but `getArea == 0.0` — such cells
      * hold zero source pixels and are dropped. Real areal overlap (even all-NoData
      * interior) is always kept.
      *
      * This is the canonical implementation; [[RasterTessellate]] delegates here and must
      * not maintain a separate copy.
      */
    def hasPositiveAreaOverlap(cellGeom: Geometry, bbox: Geometry): Boolean = {
        if (!cellGeom.intersects(bbox)) return false
        val inter = cellGeom.intersection(bbox)
        inter != null && !inter.isEmpty && inter.getArea > 0.0
    }
}
