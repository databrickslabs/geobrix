package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.{GDALBuildVRT, GDALTranslate, WarpSource}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.osr.SpatialReference

import java.nio.file.Files
import scala.util.control.NonFatal

/** Merges multiple rasters into one via VRT + gdal_translate (resolution highest). Returns (Dataset, metadata). Caller must release. */
object MergeRasters {

    /** The fraction of a pixel by which a footprint edge may lie inside a grid line and still be on it. */
    private val SnapTol = 1e-6

    /** The CRS a group in several CRSs is merged in, as the light tier picks it: the smallest EPSG code present (so
      * the same group gives the same mosaic in any order), else the first CRS there is. */
    private def referenceCrs(srs: Seq[SpatialReference]): SpatialReference = {
        val coded = srs.map(sr => (SpatialRefOps.getEPSGCode(sr), sr)).filter(_._1 > 0)
        if (coded.nonEmpty) coded.minBy(_._1)._2 else srs.head
    }

    /** `dss` with each member that is in another CRS than the group's reference CRS ([[referenceCrs]]) reprojected to
      * it, and those new members (for the caller to release). The light tier's rule for tiles in several CRSs: a
      * member is put on the pixel size of the reference tile (the first member in the reference CRS) and on its pixel
      * grid, so it can grow by up to a pixel on each side; gdalbuildvrt would skip it, and the merge would lose it
      * without a word. Members with no CRS, and a group in one CRS, are returned as they are. */
    def conformCrs(dss: Array[Dataset]): (Array[Dataset], Seq[Dataset]) = {
        val srs = dss.map(d => if (RasterProject.hasCrs(d)) Option(d.GetSpatialRef) else None)
        val present = srs.flatten.toSeq
        if (present.isEmpty || present.forall(_.IsSame(present.head) == 1)) (dss, Seq.empty)
        else {
            val ref = referenceCrs(present)
            val refTile = dss.zip(srs).collectFirst { case (d, Some(sr)) if sr.IsSame(ref) == 1 => d }.get
            val rgt = refTile.GetGeoTransform
            val (xres, yres) = (math.abs(rgt(1)), math.abs(rgt(5)))
            val made = scala.collection.mutable.ArrayBuffer.empty[Dataset]
            val wktFile = Files.createTempFile("merge_rasters_t_srs", ".wkt")
            try {
                Files.write(wktFile, ref.ExportToWkt().getBytes("UTF-8"))
                val members = dss.zip(srs).map {
                    case (d, Some(sr)) if sr.IsSame(ref) != 1 =>
                        val box = BoundingBox.densifiedBBox(d, ref).getEnvelopeInternal
                        // the grid lines of the reference tile at or beyond the footprint; an edge within SnapTol of a
                        // pixel (float noise on an aligned footprint) is on its line (the light tier's `snap_bounds`)
                        val left = rgt(0) + math.floor((box.getMinX - rgt(0)) / xres + SnapTol) * xres
                        val right = rgt(0) + math.ceil((box.getMaxX - rgt(0)) / xres - SnapTol) * xres
                        val top = rgt(3) + math.ceil((box.getMaxY - rgt(3)) / yres - SnapTol) * yres
                        val bottom = rgt(3) + math.floor((box.getMinY - rgt(3)) / yres + SnapTol) * yres
                        val out = s"/vsimem/merge_rasters_crs_${java.util.UUID.randomUUID().toString.replace("-", "_")}.tif"
                        val (warped, _) = WarpSource.warpFillAsNoData(
                          d, out,
                          s"gdalwarp -t_srs ${wktFile.toString} -te $left $bottom $right $top -tr $xres $yres -r near"
                        )
                        if (warped == null) {
                            throw new IllegalArgumentException(
                              s"gbx_rst_merge: a tile in another CRS could not be reprojected to the group's: ${gdal.GetLastErrorMsg}")
                        }
                        made += warped
                        warped
                    case (d, _) => d
                }
                (members, made.toSeq)
            } catch {
                case NonFatal(e) => made.foreach(RasterDriver.releaseDataset); throw e
            } finally Files.deleteIfExists(wktFile)
        }
    }

    /** [[merge]] of members in one CRS: those in another are reprojected first ([[conformCrs]]), as the light tier merges them. */
    def mergeConformed(dss: Array[Dataset], options: Map[String, String]): (Dataset, Map[String, String]) = {
        val (members, made) = conformCrs(dss)
        try merge(members, options)
        finally made.foreach(RasterDriver.releaseDataset)
    }

    /** Builds VRT from dss, translates to output path; returns (Dataset, metadata). */
    def merge(dss: Array[Dataset], options: Map[String, String]): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val driver = dss.head.GetDriver()
        val extension = GDAL.getExtension(driver.getShortName)
        val vrtPath = s"/vsimem/merge_rasters_$uuid.vrt"
        val rasterPath = s"/vsimem/merge_rasters_$uuid.$extension"

        val (vrtRaster, vrtOptions) = GDALBuildVRT.executeVRT(
          vrtPath,
          dss,
          options,
          command = s"gdalbuildvrt -resolution highest"
        )

        val result = GDALTranslate.executeTranslate(
          rasterPath,
          vrtRaster,
          command = s"gdal_translate",
          vrtOptions
        )

        vrtRaster.delete()
        gdal.Unlink(vrtPath)

        result
    }

}
