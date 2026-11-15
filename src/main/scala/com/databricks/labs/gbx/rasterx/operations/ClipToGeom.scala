package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.WarpSource
import com.databricks.labs.gbx.util.NodeFilePathUtil
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference
import org.locationtech.jts.geom.Geometry

import java.nio.file.{Files, Paths}
import java.util.Locale

/** Clips a raster to a JTS geometry via gdalwarp cutline; geometry is reprojected to raster CRS and buffered. */
object ClipToGeom {

    /** Clips the dataset to the geometry; returns (new Dataset, updated metadata). Caller must release the Dataset.
      * `dstNoData`, when given, fills the pixels outside the geometry and is declared on the output
      * (`-dstnodata`), and the source pixels equal to it in bands without NoData become NoData too
      * ([[GDAL.srcNoDataArgs]]); otherwise gdalwarp carries the source NoData, or fills with 0 when there
      * is none. A source with its own mask band is warped through its mask view ([[WarpSource]]), so the
      * pixels that mask hides are filled like the pixels outside the geometry. `source`, when given, is the
      * [[WarpSource]] of `ds` to warp through (a caller that clips one dataset many times builds it once and
      * closes it); otherwise the clip builds and closes its own. */
    def clip(
        ds: Dataset,
        options: Map[String, String],
        geometry: Geometry,
        geomSR: SpatialReference,
        cutlineAllTouched: Boolean = true,
        dstNoData: Option[Double] = None,
        source: Option[WarpSource] = None
    ): (Dataset, Map[String, String]) = {
        require(source.forall(_.ds eq ds), "ClipToGeom: the warp source must be the clipped dataset's")
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val outShortName = ds.GetDriver().getShortName
        val extension = GDAL.getExtension(outShortName)
        val resultFileName = s"/vsimem/clip_to_geom_$uuid.$extension"

        val cutlineToken = cutlineAllTouched.toString.toUpperCase(Locale.ROOT)
        val clipperFile = generateClipFile(geometry, geomSR, ds)
        var own: WarpSource = null
        try {
            val src = source.getOrElse { own = new WarpSource(ds); own }
            val dstNoDataFlag = dstNoData.map(nd => s" -dstnodata ${nd.toString}").getOrElse("")
            val srcNoData =
                dstNoData.map(nd => GDAL.srcNoDataArgs(ds, _ => nd, maskAsAlpha = src.masked)).getOrElse(Seq.empty)

            // For -wo consult https://gdal.org/doxygen/structGDALWarpOptions.html
            src.warp(
              resultFileName,
              options,
              // -q flag for quiet, as there is CRS warning spam
              // we already ensured geoms are in Raster CRS
              command = s"gdalwarp -q -wo CUTLINE_ALL_TOUCHED=$cutlineToken -cutline $clipperFile -crop_to_cutline" +
                  s"$dstNoDataFlag -oo GEOM_POSSIBLE_NAMES=WKT",
              extraArgs = srcNoData
            )
        } finally {
            if (own != null) own.close()
            cleanUpClipper(clipperFile)
        }
    }

    /** Writes geometry to a temp file for gdalwarp cutline; returns path. */
    private def generateClipFile(
        geometry: Geometry,
        geomCRS: SpatialReference,
        ds: Dataset
    ): String = {
        val adjustedGeom = getClipGeom(geometry, geomCRS, ds)
        val wkt = JTS.toWKT(adjustedGeom)
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val tmpFileName = s"${NodeFilePathUtil.rootPath}/$uuid/clip_$uuid.csv"
        val tmpPrjName = s"${NodeFilePathUtil.rootPath}/$uuid/clip_$uuid.prj"
        val tmpFile = Paths.get(tmpFileName)
        val tmpPrj = Paths.get(tmpPrjName)
        Files.createDirectories(tmpFile.getParent)
        val writer = Files.newBufferedWriter(tmpFile)
        try {
            writer.write(s"""|id,WKT
                             |1,"$wkt"""".stripMargin)
        } finally {
            writer.close()
        }
        Files.writeString(tmpPrj, ds.GetSpatialRef.ExportToWkt)
        tmpFile.toAbsolutePath.toString
    }

    /** Reprojects geometry to raster CRS and buffers by half a pixel for clean clip. */
    private def getClipGeom(
        geometry: Geometry,
        geomSR: SpatialReference,
        ds: Dataset
    ): Geometry = {
        val dsSR = ds.GetSpatialRef
        val geomSrcSR = if (geomSR == null) dsSR else geomSR
        val projectedGeom = OSRTransformGeometry.transform(geometry, geomSrcSR, dsSR)
        val pxXSize = Math.abs(ds.GetGeoTransform()(1))
        val pxYSize = Math.abs(ds.GetGeoTransform()(5))
        val pxDiagSize = Math.sqrt(pxXSize * pxXSize + pxYSize * pxYSize)
        val factor = 0.5 * pxDiagSize
        val pxArea = Math.abs(pxXSize * pxYSize)
        val adjustedGeom = if (projectedGeom.getArea < pxArea) projectedGeom.buffer(factor) else projectedGeom
        adjustedGeom
    }

    /** Deletes the temporary cutline file. */
    private def cleanUpClipper(fileName: String): Unit = {
        val clipFile = Paths.get(fileName)
        Files.deleteIfExists(clipFile)
        val prjFile = Paths.get(fileName.replace(".csv", ".prj"))
        Files.deleteIfExists(prjFile)
        Files.deleteIfExists(clipFile.getParent)
    }

}
