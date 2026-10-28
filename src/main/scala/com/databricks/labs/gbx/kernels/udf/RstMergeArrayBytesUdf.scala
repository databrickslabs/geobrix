package com.databricks.labs.gbx.kernels.udf

import com.databricks.labs.gbx.kernels.KernelGdal
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.MergeRasters
import org.apache.spark.sql.api.java.UDF1

import scala.util.Try
import scala.util.control.NonFatal

/** L2 finalize for collect_list(tile.raster): canonical (sorted) input order, as RST_MergeAgg sorts (:85).
  * Bytes in, bytes out; no tile Row crosses into this class. A member that does not open is dropped, as the
  * aggregate drops it. NULL when nothing is left (an error tile has no raster, so it never gets here) or the
  * mosaic fails: a Java UDF returns bytes, which have no place for a reason. */
class RstMergeArrayBytesUdf extends UDF1[Any, Array[Byte]] {
    override def call(rasters: Any): Array[Byte] = {
        val in = rasters match {
            case s: scala.collection.Seq[_] => s.collect { case b: Array[Byte] => b }
            case _ => Seq.empty[Array[Byte]]
        }
        if (in.isEmpty) return null
        KernelGdal.ensureInit()
        val opts = Map("driver" -> "GTiff")
        val dss = in.sortWith((a, b) => java.util.Arrays.compareUnsigned(a, b) < 0)
            .flatMap(b => Try(RasterDriver.readFromBytes(b, opts)).toOption.filter(_ != null)).toArray
        try {
            if (dss.isEmpty) return null
            val (out, md) = MergeRasters.merge(dss, opts)
            try RasterDriver.writeToBytes(out, md) finally RasterDriver.releaseDataset(out)
        } catch { case NonFatal(_) => null }
        finally dss.foreach(RasterDriver.releaseDataset)
    }
}
