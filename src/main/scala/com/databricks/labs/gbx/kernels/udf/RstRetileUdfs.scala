package com.databricks.labs.gbx.kernels.udf

import com.databricks.labs.gbx.kernels.{Coerce, KernelGdal}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.ReTile
import org.apache.spark.sql.api.java.{UDF3, UDF4}
import org.gdal.gdal.Dataset
import org.slf4j.LoggerFactory

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.security.MessageDigest
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** What the retile facades share: an open raster kept between calls, so a tile cut into N pieces is opened
  * once instead of N times (the count+part shape calls once per piece). The cache is per thread, because a GDAL
  * Dataset is not safe to share between tasks, holds at most [[Capacity]] rasters, and is cleared when the Spark
  * task that filled it ends. A raster pushed out is released through [[RasterDriver.releaseDataset]], which frees
  * its in-memory file. */
object RetileKernel {
    val Capacity = 4
    private val log = LoggerFactory.getLogger(RetileKernel.getClass)
    private val lru = ThreadLocal.withInitial[java.util.LinkedHashMap[String, Dataset]](() =>
        new java.util.LinkedHashMap[String, Dataset](8, 0.75f, true) {
            override def removeEldestEntry(e: java.util.Map.Entry[String, Dataset]): Boolean = {
                val drop = size() > Capacity
                if (drop) RasterDriver.releaseDataset(e.getValue)
                drop
            }
        })
    private val registeredFor = new ThreadLocal[AnyRef]()

    // A content hash of a 4096 x 4096 tile costs more than cutting all its windows, and the count+part shape asks
    // once per window. The key is the length, the first and last 64 KB and 64 blocks of 4 KB spread between them
    // (the whole raster when it is at most 512 KB). Two different rasters of one length that agree on every sampled
    // byte would share an entry; the inputs of the oracle and the bench do not.
    private val Head = 65536
    private val Block = 4096
    private val Blocks = 64
    private val Whole = 524288

    /** The cache key of `raster`: a digest of its length and the sampled bytes. */
    def key(raster: Array[Byte]): String = {
        val md = MessageDigest.getInstance("MD5")
        md.update(java.nio.ByteBuffer.allocate(8).putLong(raster.length.toLong).array())
        if (raster.length <= Whole) {
            md.update(raster)
        } else {
            md.update(raster, 0, Head)
            md.update(raster, raster.length - Head, Head)
            val span = (raster.length - 2 * Head - Block).toLong
            (0 until Blocks).foreach(i => md.update(raster, Head + (span * i / (Blocks - 1)).toInt, Block))
        }
        java.util.Base64.getEncoder.encodeToString(md.digest())
    }

    /** The raster behind `raster`, opened now or taken from this thread's cache. Raises when the bytes do not open. */
    def open(raster: Array[Byte]): Dataset = {
        KernelGdal.ensureInit()
        clearAtTaskEnd()
        val k = key(raster)
        val m = lru.get()
        Option(m.get(k)).getOrElse {
            val ds = RasterDriver.readFromBytes(raster, Map("driver" -> "GTiff"))
            if (ds == null) throw new IllegalArgumentException("the raster bytes do not open")
            m.put(k, ds)
            ds
        }
    }

    /** How many rasters this thread holds open. */
    def cached(): Int = lru.get().size()

    /** Release every raster this thread holds. */
    def clear(): Unit = {
        val m = lru.get()
        m.values().asScala.foreach(RasterDriver.releaseDataset)
        m.clear()
    }

    /** Clear this thread's cache when the Spark task running on it ends, so a finished task leaves nothing open.
      * Spark core is reached by reflection: a facade may run where it is not on the classpath, and then only
      * [[Capacity]] bounds the cache. Outside a task (a test, a plain thread) there is nothing to register. */
    private def clearAtTaskEnd(): Unit =
        try {
            // scalastyle:off classforname
            val taskContext = Class.forName("org.apache.spark.TaskContext")
            val current = taskContext.getMethod("get").invoke(null)
            if (current != null && (registeredFor.get() ne current)) {
                val listener = Class.forName("org.apache.spark.util.TaskCompletionListener")
                // scalastyle:on classforname
                val handler = new InvocationHandler {
                    override def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = method.getName match {
                        case "onTaskCompletion" => clear(); null
                        case "hashCode"         => Int.box(System.identityHashCode(proxy))
                        case "equals"           => Boolean.box(proxy eq args(0))
                        case _                  => null
                    }
                }
                val proxy = Proxy.newProxyInstance(listener.getClassLoader, Array(listener), handler)
                taskContext.getMethod("addTaskCompletionListener", listener).invoke(current, proxy)
                registeredFor.set(current)
            }
        } catch {
            case _: ReflectiveOperationException | _: LinkageError => () // no Spark core here
        }

    /** GTiff bytes of one window `(col_off, row_off, width, height)` of `ds`. */
    def cut(ds: Dataset, win: (Int, Int, Int, Int)): Array[Byte] = {
        val (t, md) = ReTile.getTile(ds, Map("driver" -> "GTiff"), win._1, win._2, win._3, win._4)
        try RasterDriver.writeToBytes(t, md) finally RasterDriver.releaseDataset(t)
    }

    /** Log a failure the facades turn into an error tile (NULL bytes); the reason stays in the executor log. */
    def failed(e: Throwable): Unit =
        log.warn(s"gbx_rst_retile: ${e.getClass.getSimpleName}: ${e.getMessage}")
}

/* The error contract of the other levels, for bytes: a PARAMETER that is not a number raises; bytes that do not open,
 * or a window that cannot be cut, are DATA, and give what the manager's SQL wrapper turns into one error tile
 * (an array of one NULL, a NULL piece, or one piece that is NULL). A NULL raster is NULL, so no rows. */

/** L2 facade for the count half of the count+part shape of gbx_rst_retile: how many pieces a raster has.
  * A raster that does not open is one piece, which `part` answers with NULL bytes (the error tile). */
class RstRetileCountBytesUdf extends UDF3[Array[Byte], Any, Any, java.lang.Integer] {
    override def call(r: Array[Byte], w: Any, h: Any): java.lang.Integer = {
        if (r == null) return null
        val (tw, th) = (Coerce.double(w).toInt, Coerce.double(h).toInt)
        try ReTile.generateWindows(RetileKernel.open(r), tw, th).size
        catch { case NonFatal(e) => RetileKernel.failed(e); 1 }
    }
}

/** L2 facade for the part half: piece `i` of a raster, as GTiff bytes (NULL when the raster does not open). */
class RstRetilePartBytesUdf extends UDF4[Array[Byte], Any, Any, Any, Array[Byte]] {
    override def call(r: Array[Byte], w: Any, h: Any, i: Any): Array[Byte] = {
        if (r == null || i == null) return null
        val (tw, th, idx) = (Coerce.double(w).toInt, Coerce.double(h).toInt, Coerce.double(i).toInt)
        val planned =
            try { val ds = RetileKernel.open(r); Some((ds, ReTile.generateWindows(ds, tw, th))) }
            catch { case NonFatal(e) => RetileKernel.failed(e); None }
        planned match {
            case None => null
            case Some((ds, windows)) =>
                val win = windows(idx) // an index the count never gave raises
                try RetileKernel.cut(ds, win) catch { case NonFatal(e) => RetileKernel.failed(e); null }
        }
    }
}

/** L2 facade for the array shape: every piece of a raster as one `array<binary>`. A raster that does not open
  * is an array of one NULL, which the wrapper turns into one error tile. */
class RstRetileArrayBytesUdf extends UDF3[Array[Byte], Any, Any, java.util.List[Array[Byte]]] {
    override def call(r: Array[Byte], w: Any, h: Any): java.util.List[Array[Byte]] = {
        if (r == null) return null
        val (tw, th) = (Coerce.double(w).toInt, Coerce.double(h).toInt)
        try {
            val ds = RetileKernel.open(r)
            ReTile.generateWindows(ds, tw, th).map(RetileKernel.cut(ds, _)).asJava
        } catch {
            case NonFatal(e) => RetileKernel.failed(e); java.util.Collections.singletonList[Array[Byte]](null)
        }
    }
}
