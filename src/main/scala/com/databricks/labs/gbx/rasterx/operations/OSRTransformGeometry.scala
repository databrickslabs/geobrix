package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.ogr.{Geometry => OGRGeometry}
import org.gdal.osr.{SpatialReference, osrConstants}
import org.locationtech.jts.geom.{Geometry => JTSGeometry}

/** Reprojects a JTS geometry from one OSR SpatialReference to another via OGR. */
object OSRTransformGeometry {

    /** Transforms the geometry from srcSR to dstSR; returns the same geometry if CRS are equal.
     *
     *  JTS WKB/WKT always store coordinates in (x, y) = (lon, lat) order ("traditional GIS order"),
     *  but GDAL 3+ defaults an EPSG-imported `SpatialReference` to authority-compliant axis order —
     *  so for e.g. EPSG:4326 GDAL would treat the first coord as latitude and the second as
     *  longitude, flipping input silently and producing clips that miss the raster footprint
     *  (all-black output). To protect callers we clone both SRs and force traditional axis order
     *  on the clones before the transform runs. Cloning keeps the caller-owned SRs unmutated.
     */
    def transform(
        geom: JTSGeometry,
        srcSR: SpatialReference,
        dstSR: SpatialReference
    ): JTSGeometry = {
        if (srcSR.IsSame(dstSR) == 1) return geom
        val srcClone = srcSR.Clone()
        val dstClone = dstSR.Clone()
        srcClone.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        dstClone.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        try {
            val ogrGeom = OGRGeometry.CreateFromWkb(JTS.toWKBAdaptive(geom))
            ogrGeom.AssignSpatialReference(srcClone)
            ogrGeom.TransformTo(dstClone)
            val res = try JTS.fromWKB(ogrGeom.ExportToWkb()) finally ogrGeom.delete()
            res
        } finally {
            srcClone.delete()
            dstClone.delete()
        }
    }

}
