package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.ogr.{Geometry => OGRGeometry}
import org.gdal.osr.SpatialReference
import org.locationtech.jts.geom.{Geometry => JTSGeometry}

/** Reprojects a JTS geometry from one OSR SpatialReference to another via OGR. */
object OSRTransformGeometry {

    /** Transforms the geometry from srcSR to dstSR; returns the same geometry if CRS are equal. */
    def transform(
        geom: JTSGeometry,
        srcSR: SpatialReference,
        dstSR: SpatialReference
    ): JTSGeometry = {
        if (srcSR.IsSame(dstSR) == 1) return geom
        val ogrGeom = OGRGeometry.CreateFromWkb(JTS.toWKB(geom))
        ogrGeom.AssignSpatialReference(srcSR)
        ogrGeom.TransformTo(dstSR)
        val res = JTS.fromWKB(ogrGeom.ExportToWkb())
        ogrGeom.delete()
        res
    }

}
