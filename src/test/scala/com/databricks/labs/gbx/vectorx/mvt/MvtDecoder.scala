package com.databricks.labs.gbx.vectorx.mvt

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.gdal
import org.gdal.ogr.ogr.{GetDriverByName => OGRGetDriverByName}
import org.gdal.ogr.ogrConstants

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * Decode a Mapbox Vector Tile (MVT) protobuf blob into features: the tests' round-trip check of [[MvtWriter]].
  *
  * Uses GDAL's OGR MVT driver opened in read mode via a `/vsimem/` scratch path.
  * The OGR MVT driver accepts a single `.pbf` file path for reading (not a directory
  * — the directory layout is only used during CREATION by the write side). We write
  * the blob bytes to a `/vsimem/<uuid>.pbf` file and open it directly. Resource
  * management mirrors `MvtWriter`: every Dataset and Feature is `.delete()`'d before
  * returning, and the `/vsimem/` file is unlinked via `gdal.Unlink`.
  *
  * GDAL thread-safety: OGR drivers are registered via the synchronized
  * `GDALManager.initOgr()` guard (CLAUDE.md requirement). The `/vsimem/` paths are
  * UUID-namespaced to avoid collisions across concurrent Spark tasks.
  *
  * Returns `Seq[(layerName, geom_wkb, attrs)]`. When a raw `.pbf` file is opened
  * without z/x/y tile context, the OGR MVT driver returns coordinates as raw integer
  * tile-local values in `[0, extent]` pixel space — matching the input contract of
  * `MvtWriter.encode`. Features with null or empty geometries are skipped. Returns an
  * empty Seq for an empty blob or any undecodable input — never throws.
  */
object MvtDecoder {

    /**
      * Decode `blob` into a flat sequence of `(layerName, geomWkb, attrs)` tuples.
      *
      * @param blob MVT protobuf bytes.
      * @return All features across all layers; empty Seq if the blob is empty or
      *         cannot be decoded. Returned WKB is in tile-local `[0, extent]` pixel
      *         space (raw integer coordinates as stored in the MVT protobuf, matching
      *         the input contract of `MvtWriter.encode`).
      */
    def decode(blob: Array[Byte]): Seq[(String, Array[Byte], Map[String, Any])] = {
        if (blob == null || blob.isEmpty) return Seq.empty
        // Load the GDAL native library and register OGR drivers under the shared lock.
        GDALManager.ensureJniLoaded(None)
        GDALManager.initOgr()

        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        // The OGR MVT driver reads a single .pbf file directly (not a directory).
        val pbfPath = s"/vsimem/gbx_mvtdec_$uuid.pbf"
        gdal.FileFromMemBuffer(pbfPath, blob)

        val result = ArrayBuffer.empty[(String, Array[Byte], Map[String, Any])]
        val driver = OGRGetDriverByName("MVT")
        if (driver == null) {
            Try(gdal.Unlink(pbfPath))
            return Seq.empty
        }

        val ds = Try(driver.Open(pbfPath, 0)).toOption.orNull
        if (ds == null) {
            Try(gdal.Unlink(pbfPath))
            return Seq.empty
        }

        try {
            val layerCount = ds.GetLayerCount()
            var li = 0
            while (li < layerCount) {
                val layer = ds.GetLayer(li)
                if (layer != null) {
                    val layerName = layer.GetName()
                    layer.ResetReading()
                    var feat = layer.GetNextFeature()
                    while (feat != null) {
                        try {
                            val geom = feat.GetGeometryRef()
                            if (geom != null) {
                                val wkb = geom.ExportToWkb()
                                if (wkb != null && wkb.nonEmpty) {
                                    result += ((layerName, wkb, readAttrs(feat)))
                                }
                            }
                        } finally {
                            feat.delete()
                        }
                        feat = layer.GetNextFeature()
                    }
                }
                li += 1
            }
        } finally {
            ds.delete()
            Try(gdal.Unlink(pbfPath))
        }
        result.toSeq
    }

    /** Extract all field values from a feature as `Map[String, Any]` with native types. */
    private def readAttrs(feat: org.gdal.ogr.Feature): Map[String, Any] = {
        val defn = feat.GetDefnRef()
        val count = defn.GetFieldCount()
        val m = scala.collection.mutable.Map.empty[String, Any]
        var i = 0
        while (i < count) {
            val fieldDefn = defn.GetFieldDefn(i)
            val name = fieldDefn.GetNameRef()
            val ft = fieldDefn.GetFieldType()
            val value: Any =
                if (ft == ogrConstants.OFTInteger)        feat.GetFieldAsInteger(i)
                else if (ft == ogrConstants.OFTInteger64) feat.GetFieldAsInteger64(i)
                else if (ft == ogrConstants.OFTReal)      feat.GetFieldAsDouble(i)
                else                                      feat.GetFieldAsString(i)
            m(name) = value
            i += 1
        }
        m.toMap
    }
}
