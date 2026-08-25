package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.rasterx.operations.SpatialRefOps
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants.GDT_Float64
import org.gdal.ogr.{DataSource, Feature, FeatureDefn, FieldDefn, Geometry, Layer, ogr}
import org.gdal.ogr.ogrConstants.{OFTReal, wkbUnknown}

import java.util.UUID

/** Shared helpers for the vector↔raster bridge expressions
 *  (`RST_Rasterize` and `RST_Polygonize`).
 *
 *  These wrap GDAL's `Memory` OGR driver, `MEM` raster driver, and GTiff
 *  serialization in three single-purpose methods so the two expressions can
 *  stay focused on their orchestration logic.
 *
 *  Resource ownership convention: every method that returns a native GDAL
 *  object documents what the caller is responsible for releasing. Forgetting
 *  to `.delete()` a Dataset or DataSource leaks native memory.
 */
object VectorRasterBridge {

    /** Field name used for the burn value attribute on the in-memory OGR layer. */
    val ValueFieldName: String = "value"

    /** Build an in-memory OGR Layer from `(geom_wkb, value)` tuples.
     *
     *  Returns the (DataSource, Layer) pair; caller must call `.delete()` on
     *  the DataSource when done — that releases the layer too.
     */
    def buildOgrLayer(
        features: Seq[(Array[Byte], Double)],
        srid: Int
    ): (DataSource, Layer) = {
        GDALManager.initOgr()
        val driver = ogr.GetDriverByName("Memory")
        val ds = driver.CreateDataSource(s"mem_${UUID.randomUUID().toString.replace("-", "")}")
        // Resolve via the shared rule so an ESRI code (e.g. 54008) classifies
        // correctly, not only EPSG — matches the light tier's out_srid handling.
        val sr = SpatialRefOps.resolveCrs(srid.toString)
        val layer = ds.CreateLayer("features", sr, wkbUnknown)
        val fd = new FieldDefn(ValueFieldName, OFTReal)
        layer.CreateField(fd); fd.delete()
        val defn: FeatureDefn = layer.GetLayerDefn()
        features.foreach { case (wkb, v) =>
            val feat = new Feature(defn)
            val geom = Geometry.CreateFromWkb(wkb)
            if (geom != null) {
                feat.SetGeometry(geom)
                feat.SetField(ValueFieldName, v)
                layer.CreateFeature(feat)
                geom.delete()
            }
            feat.delete()
        }
        sr.delete()
        (ds, layer)
    }

    /** Create an empty in-memory raster `Dataset` of the requested extent, size, and SRID.
     *
     *  Caller is responsible for `.delete()`.
     */
    def buildEmptyRaster(
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, srid: Int,
        noDataValue: Double = -9999.0
    ): Dataset = {
        require(widthPx > 0, s"rst_rasterize: width_px must be positive; got $widthPx")
        require(heightPx > 0, s"rst_rasterize: height_px must be positive; got $heightPx")
        require(xmax > xmin, s"rst_rasterize: xmax ($xmax) must be > xmin ($xmin)")
        require(ymax > ymin, s"rst_rasterize: ymax ($ymax) must be > ymin ($ymin)")
        val memDriver = gdal.GetDriverByName("MEM")
        val ds = memDriver.Create("", widthPx, heightPx, 1, GDT_Float64)
        val xRes = (xmax - xmin) / widthPx
        val yRes = (ymax - ymin) / heightPx
        ds.SetGeoTransform(Array(xmin, xRes, 0.0, ymax, 0.0, -yRes))
        // Resolve via the shared rule so ESRI codes classify correctly (not just EPSG).
        val sr = SpatialRefOps.resolveCrs(srid.toString)
        ds.SetProjection(sr.ExportToWkt())
        sr.delete()
        val band = ds.GetRasterBand(1)
        band.SetNoDataValue(noDataValue)
        band.Fill(noDataValue)
        ds
    }

    /** Copy `ds` to a GTiff `/vsimem/` path, read the bytes back, then unlink.
     *
     *  We materialize through GTiff because the RasterX tile invariant is that
     *  binary tiles carry a GTiff-compatible byte stream (the `MEM` driver
     *  produces no bytes — there is nothing to read from `/vsimem/`).
     */
    def toGTiffBytes(ds: Dataset): Array[Byte] = {
        val outPath = s"/vsimem/vrbridge_${UUID.randomUUID().toString.replace("-", "")}.tif"
        val gtiffDriver = gdal.GetDriverByName("GTiff")
        val out = gtiffDriver.CreateCopy(outPath, ds)
        out.FlushCache()
        out.delete()
        val bytes = gdal.GetMemFileBuffer(outPath)
        gdal.Unlink(outPath)
        if (bytes == null) Array.emptyByteArray else bytes
    }

}
