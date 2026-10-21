package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import com.databricks.labs.gbx.util.NodeFilePathUtil
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.{Dataset, TranslateOptions, gdal}
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
      * is none. A source with its own mask band is warped through its [[maskView]], so the pixels that
      * mask hides are filled like the pixels outside the geometry. */
    def clip(
        ds: Dataset,
        options: Map[String, String],
        geometry: Geometry,
        geomSR: SpatialReference,
        cutlineAllTouched: Boolean = true,
        dstNoData: Option[Double] = None
    ): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val outShortName = ds.GetDriver().getShortName
        val extension = GDAL.getExtension(outShortName)
        val resultFileName = s"/vsimem/clip_to_geom_$uuid.$extension"

        val cutlineToken = cutlineAllTouched.toString.toUpperCase(Locale.ROOT)
        val clipperFile = generateClipFile(geometry, geomSR, ds)
        var view: Dataset = null
        try {
            view = maskView(ds).orNull
            val masked = view != null
            val dstNoDataFlag = dstNoData.map(nd => s" -dstnodata ${nd.toString}").getOrElse("")
            val srcNoData = dstNoData.map(nd => GDAL.srcNoDataArgs(ds, _ => nd, maskAsAlpha = masked)).getOrElse(Seq.empty)
            val srcAlpha = if (masked) Seq("-srcalpha") else Seq.empty

            // For -wo consult https://gdal.org/doxygen/structGDALWarpOptions.html
            GDALWarp.executeWarp(
              resultFileName,
              Array(if (masked) view else ds),
              options,
              // -q flag for quiet, as there is CRS warning spam
              // we already ensured geoms are in Raster CRS
              command = s"gdalwarp -q -wo CUTLINE_ALL_TOUCHED=$cutlineToken -cutline $clipperFile -crop_to_cutline" +
                s"$dstNoDataFlag -oo GEOM_POSSIBLE_NAMES=WKT",
              extraArgs = srcAlpha ++ srcNoData,
              like = ds
            )
        } finally {
            // The view owns no file, so delete() releases all of it (as for the VRTs of MergeRasters and
            // MergeBands). RasterDriver.releaseDataset must not be used: a VRT's file list names its SOURCE
            // files, which it would unlink (/vsimem) or whose node-cache read lock it would drop.
            if (view != null) view.delete()
            cleanUpClipper(clipperFile)
        }
    }

    /**
      * For a source masked by its own per-dataset mask band ([[GDAL.hasOwnMask]]: an internal GTiff mask,
      * as JPEG/YCbCr imagery often has, not an alpha band), an in-memory VRT of `ds`: its bands, then that
      * mask as one more band (colour interpretation undefined) that the warp reads as the source alpha
      * (`-srcalpha`). gdalwarp ignores a per-dataset mask band under a cutline it rasterizes (a hexagon,
      * for example) and whenever a source NoData is set, but it always applies a source alpha band. As the
      * extra band is not marked alpha, gdalwarp adds no alpha band to the output, which keeps the source's
      * bands and data type. None for any other source: no own mask, or an alpha band, which gdalwarp
      * already applies. The view reads `ds` through its band handles and owns no file; delete() it (only)
      * before `ds` is released.
      */
    private def maskView(ds: Dataset): Option[Dataset] = {
        val n = ds.GetRasterCount()
        val maskOf = (1 to n).find(i => GDAL.hasOwnMask(ds.GetRasterBand(i)))
        if (maskOf.isEmpty || GDAL.hasAlpha(ds)) None
        else {
            val args = new java.util.Vector[String]()
            (Seq("-of", "VRT") ++ (1 to n).flatMap(i => Seq("-b", i.toString)) ++
                Seq("-b", s"mask,${maskOf.get}", s"-colorinterp_${n + 1}", "undefined", "-mask", "none"))
                .foreach(args.add)
            val translateOptions = new TranslateOptions(args)
            val view =
                try gdal.Translate("", ds, translateOptions)
                finally translateOptions.delete()
            if (view == null) {
                throw new IllegalStateException(
                  s"ClipToGeom: could not build the mask view of ${ds.GetDescription}: ${gdal.GetLastErrorMsg}"
                )
            }
            Some(view)
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
