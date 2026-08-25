package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.{operations => gbxops}
import org.gdal.osr.{CoordinateTransformation, SpatialReference}

/** Forwarder — the real implementation lives in `com.databricks.labs.gbx.operations.SpatialRefOps`
  * (tier-neutral). This object re-exposes every public member so that the ~12 rasterx importers
  * (RST_Clip, RST_Sample, RST_Viewshed, RST_SetSrid, RST_SetCrs, RST_TransformCrs,
  * RST_Crs, RST_H3_CellBBox, RST_GridFromPoints, VectorRasterBridge, GDAL_Batch,
  * GDALRasterize) compile unchanged. */
object SpatialRefOps {

    def getEPSGCode(spatialRef: SpatialReference): Int =
        gbxops.SpatialRefOps.getEPSGCode(spatialRef)

    def resolveCrs(value: String): SpatialReference =
        gbxops.SpatialRefOps.resolveCrs(value)

    def crsToCanonical(spatialRef: SpatialReference): String =
        gbxops.SpatialRefOps.crsToCanonical(spatialRef)

    def getTransformer(srcKey: String, dstKey: String): CoordinateTransformation =
        gbxops.SpatialRefOps.getTransformer(srcKey, dstKey)

    def resolveSourceSR(
        embeddedSrid: Int, srid: Option[Int], crs: Option[String]
    ): Option[SpatialReference] =
        gbxops.SpatialRefOps.resolveSourceSR(embeddedSrid, srid, crs)

    def fromEPSGCode(getSRID: Int): SpatialReference =
        gbxops.SpatialRefOps.fromEPSGCode(getSRID)

}
