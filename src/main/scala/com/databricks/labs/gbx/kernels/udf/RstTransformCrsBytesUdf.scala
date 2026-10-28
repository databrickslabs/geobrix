package com.databricks.labs.gbx.kernels.udf

import com.databricks.labs.gbx.kernels.KernelGdal
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.{RasterProject, SpatialRefOps}
import org.apache.spark.sql.api.java.UDF2
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

/** L2 facade for gbx_rst_transformcrs on raster BYTES; the manager's SQL wrapper rebuilds the tile.
  * Bytes in, bytes out is the pattern that passed on DCS standard (no Row crosses into this class).
  * Any failure gives NULL bytes, which the wrapper turns into an error tile; the reason is logged here. */
class RstTransformCrsBytesUdf extends UDF2[Array[Byte], String, Array[Byte]] {
    override def call(raster: Array[Byte], crs: String): Array[Byte] = {
        if (raster == null || crs == null) return null
        try {
            KernelGdal.ensureInit()
            val dstSR = SpatialRefOps.resolveCrs(crs)
            val ds = RasterDriver.readFromBytes(raster, Map("driver" -> "GTiff"))
            if (ds == null) return null
            try {
                val (out, md) = RasterProject.project(ds, Map("driver" -> "GTiff"), dstSR)
                try RasterDriver.writeToBytes(out, md) finally RasterDriver.releaseDataset(out)
            } finally RasterDriver.releaseDataset(ds)
        } catch {
            case NonFatal(e) =>
                RstTransformCrsBytesUdf.log.warn(s"gbx_rst_transformcrs: ${e.getClass.getSimpleName}: ${e.getMessage}")
                null
        }
    }
}

object RstTransformCrsBytesUdf {
    private val log = LoggerFactory.getLogger(classOf[RstTransformCrsBytesUdf])
}
