package com.databricks.labs.gbx.kernels.udf

import com.databricks.labs.gbx.kernels.{Coerce, KernelGdal}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.ReTile
import org.apache.spark.sql.api.java.{UDF3, UDF4}
import org.gdal.gdal.Dataset

import java.security.MessageDigest
import scala.jdk.CollectionConverters._

/** What the retile facades share: an open raster kept between calls, so a tile cut into N pieces is opened
  * once instead of N times (the count+part shape calls once per piece). The cache is per thread, because a GDAL
  * Dataset is not safe to share between tasks, and holds at most [[Capacity]] rasters; a raster pushed out is
  * released through [[RasterDriver.releaseDataset]], which frees its in-memory file. */
object RetileKernel {
    val Capacity = 4
    private val lru = ThreadLocal.withInitial[java.util.LinkedHashMap[String, Dataset]](() =>
        new java.util.LinkedHashMap[String, Dataset](8, 0.75f, true) {
            override def removeEldestEntry(e: java.util.Map.Entry[String, Dataset]): Boolean = {
                val drop = size() > Capacity
                if (drop) RasterDriver.releaseDataset(e.getValue)
                drop
            }
        })

    /** The raster behind `raster`, opened now or taken from this thread's cache. */
    def open(raster: Array[Byte]): Dataset = {
        KernelGdal.ensureInit()
        val k = java.util.Base64.getEncoder.encodeToString(MessageDigest.getInstance("MD5").digest(raster))
        val m = lru.get()
        Option(m.get(k)).getOrElse { val ds = RasterDriver.readFromBytes(raster, Map("driver" -> "GTiff")); m.put(k, ds); ds }
    }

    /** How many rasters this thread holds open. */
    def cached(): Int = lru.get().size()

    /** Release every raster this thread holds. */
    def clear(): Unit = {
        val m = lru.get()
        m.values().asScala.foreach(RasterDriver.releaseDataset)
        m.clear()
    }

    /** GTiff bytes of one window `(col_off, row_off, width, height)` of `ds`. */
    def cut(ds: Dataset, win: (Int, Int, Int, Int)): Array[Byte] = {
        val (t, md) = ReTile.getTile(ds, Map("driver" -> "GTiff"), win._1, win._2, win._3, win._4)
        try RasterDriver.writeToBytes(t, md) finally RasterDriver.releaseDataset(t)
    }
}

/** L2 facade for the count half of the count+part shape of gbx_rst_retile: how many pieces a raster has. */
class RstRetileCountBytesUdf extends UDF3[Array[Byte], Any, Any, java.lang.Integer] {
    override def call(r: Array[Byte], w: Any, h: Any): java.lang.Integer =
        if (r == null) null
        else ReTile.generateWindows(RetileKernel.open(r), Coerce.double(w).toInt, Coerce.double(h).toInt).size
}

/** L2 facade for the part half: piece `i` of a raster, as GTiff bytes. */
class RstRetilePartBytesUdf extends UDF4[Array[Byte], Any, Any, Any, Array[Byte]] {
    override def call(r: Array[Byte], w: Any, h: Any, i: Any): Array[Byte] = {
        if (r == null || i == null) return null
        val ds = RetileKernel.open(r)
        RetileKernel.cut(ds, ReTile.generateWindows(ds, Coerce.double(w).toInt, Coerce.double(h).toInt)(Coerce.double(i).toInt))
    }
}

/** L2 facade for the array shape: every piece of a raster as one `array<binary>`. */
class RstRetileArrayBytesUdf extends UDF3[Array[Byte], Any, Any, java.util.List[Array[Byte]]] {
    override def call(r: Array[Byte], w: Any, h: Any): java.util.List[Array[Byte]] = {
        if (r == null) return null
        val ds = RetileKernel.open(r)
        ReTile.generateWindows(ds, Coerce.double(w).toInt, Coerce.double(h).toInt).map(RetileKernel.cut(ds, _)).asJava
    }
}
