package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.gridx.grid.CustomGridSystem
import org.gdal.gdal.Dataset
import org.gdal.osr.{SpatialReference, osrConstants}

/** Determinism guard for custom-grid raster ops.
  *
  * Validates UP FRONT that a [[CustomGridSystem]]'s bounds contain the raster extent, throwing a
  * clear [[IllegalArgumentException]] that names the offending bound — instead of letting an
  * out-of-bounds pixel centroid throw mid-aggregation. [[CustomGridSystem.pointToCellID]] require-checks
  * `x ∈ [boundXMin, boundXMax)` and `y ∈ [boundYMin, boundYMax)`; without this pre-check a raster that
  * spills past an upper bound only fails partway through the pixel loop, with a message that names a raw
  * coordinate rather than the grid bound at fault.
  *
  * The raster extent is computed in the grid's native CRS (via [[BoundingBox.bbox]], which reprojects
  * the raster corners into the target SR), so the comparison is apples-to-apples with the grid bounds
  * regardless of the raster's own CRS. Called by both the custom raster→grid and custom tessellate entries.
  */
object CustomGridBounds {

    /** Throws [[IllegalArgumentException]] naming the offending bound if the raster extent (in the grid's
      * CRS) is not fully contained within `[boundXMin, boundXMax) × [boundYMin, boundYMax)`.
      */
    def requireBoundsContainRaster(grid: CustomGridSystem, ds: Dataset): Unit = {
        val conf   = grid.conf
        val gridSR = new SpatialReference()
        gridSR.ImportFromEPSG(grid.crsSrid)
        gridSR.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        val env =
            try BoundingBox.bbox(ds, gridSR).getEnvelopeInternal
            finally gridSR.delete()

        def fail(bound: String, boundValue: Long, reach: Double): Nothing =
            throw new IllegalArgumentException(
                s"gbx_rst_custom: raster extent exceeds the custom grid bound $bound=$boundValue " +
                s"(raster reaches $reach in EPSG:${grid.crsSrid}). The grid's bounds must contain the raster extent.")

        if (env.getMinX < conf.boundXMin) fail("bound_x_min", conf.boundXMin, env.getMinX)
        if (env.getMaxX > conf.boundXMax) fail("bound_x_max", conf.boundXMax, env.getMaxX)
        if (env.getMinY < conf.boundYMin) fail("bound_y_min", conf.boundYMin, env.getMinY)
        if (env.getMaxY > conf.boundYMax) fail("bound_y_max", conf.boundYMax, env.getMaxY)
    }
}
