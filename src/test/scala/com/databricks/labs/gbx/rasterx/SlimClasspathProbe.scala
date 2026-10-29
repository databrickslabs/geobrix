package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.kernels.KernelGdal
import com.databricks.labs.gbx.kernels.uc.BngUc
import com.databricks.labs.gbx.kernels.udf.{BngEastNorthAsBngUdf, BngTessellateUdf, RstMergeArrayBytesUdf}
import com.databricks.labs.gbx.rasterx.expressions.agg.mvp.BngRasterizeCellsBytesUdf
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.apache.spark.sql.Row

import scala.jdk.CollectionConverters._

/** Run by [[KernelFacadesSmokeTest]] in a child JVM whose classpath is the facades, scala-library, JTS, the GDAL
  * bindings, spark-sql-api with what its `Row` needs, and what the GDAL start-up needs of Spark core, Hadoop and log4j
  * (what a standard-mode or Unity Catalog sandbox offers) but NOT Catalyst, the unsafe types or Kryo. It says which of
  * those could load (none should), and runs the raster facades end to end. */
object SlimClasspathProbe {

    private def attempt(body: => String): String =
        try body catch { case t: Throwable => "RAISES " + t.getClass.getSimpleName + ": " + t.getMessage }

    private val rasterize = new BngRasterizeCellsBytesUdf()

    /** A 2 x 2 km canvas of 1 km pixels from `xmin` east, 180 to 182 km north. */
    private def burn(cells: Seq[Row], xmin: Double): Array[Byte] =
        rasterize.call(cells, xmin, 180000.0, xmin + 2000.0, 182000.0, 2, 2)

    private def size(bytes: Array[Byte]): (Int, Int) = {
        val ds = RasterDriver.readFromBytes(bytes, Map("driver" -> "GTiff"))
        try (ds.getRasterXSize, ds.getRasterYSize) finally RasterDriver.releaseDataset(ds)
    }

    /** The first band as rows of pixels, `;` between rows. */
    private def pixels(bytes: Array[Byte]): String = {
        val ds = RasterDriver.readFromBytes(bytes, Map("driver" -> "GTiff"))
        try {
            val (w, h) = (ds.getRasterXSize, ds.getRasterYSize)
            val all = new Array[Double](w * h)
            ds.GetRasterBand(1).ReadRaster(0, 0, w, h, all)
            all.grouped(w).map(_.mkString(",")).mkString(";")
        } finally RasterDriver.releaseDataset(ds)
    }

    def main(args: Array[String]): Unit =
        if (args.headOption.contains("start-up")) startUp() else facades()

    /** The facades' GDAL start-up (KernelGdal: no session, an empty configuration), then the JNI it loaded. */
    private def startUp(): Unit = {
        // scalastyle:off println
        println("no-config start-up: " + attempt {
            KernelGdal.ensureInit()
            "loaded " + GDALManager.ensureJniLoaded(None).jni
        })
        // scalastyle:on println
    }

    private def facades(): Unit = {
        val udf = new BngEastNorthAsBngUdf()
        val tessellate = new BngTessellateUdf()
        val offset = "POLYGON((530100 180100, 532900 180100, 532900 181900, 530100 181900, 530100 180100))"
        val absent = Seq("org.apache.spark.sql.catalyst.InternalRow", "org.apache.spark.unsafe.types.UTF8String", "com.esotericsoftware.kryo.Kryo")
        // scalastyle:off classforname
        val loadable = absent.filter(n => try { Class.forName(n); true } catch { case _: Throwable => false })
        // scalastyle:on classforname
        val lines = Seq(
          "L2 good: " + attempt(udf.call(530000.0, 180000.0, Integer.valueOf(3))),
          "L2 bad: " + attempt(udf.call(530000.0, 180000.0, Integer.valueOf(1001))),
          "UC good: " + attempt(BngUc.eastNorthAsBng(530000.0, 180000.0, "-3")),
          "UC bad: " + attempt(BngUc.eastNorthAsBng(530000.0, 180000.0, "1001")),
          "L2 tessellate good: " + attempt {
              val chips = tessellate.call(offset, Integer.valueOf(3), java.lang.Boolean.FALSE)
              chips.size + " " + chips.asScala.map(_.getString(0)).min // six chips; the lowest cell id
          },
          "L2 tessellate bad: " + attempt(tessellate.call(offset, "3km", java.lang.Boolean.FALSE).size.toString),
          // The raster facades run end to end, GDAL natives included (the image has them): a canvas burned from cells and
          // two canvases merged, the pixels read back. A Catalyst or unsafe class anywhere on their path would raise here.
          "L2 rasterize: " + attempt(pixels(burn(Seq(
            Row("TQ3080", 3.0), Row("TQ3080", 1.0), Row("TQ3180", 2.0),
            Row("TQ3081", Double.NaN), Row("TQ3081", 7.0), Row("TQ3181", Double.NaN)), 530000.0))),
          "L2 merge: " + attempt {
              val merged = new RstMergeArrayBytesUdf().call(Seq(burn(Seq(Row("TQ3080", 1.0)), 530000.0), burn(Seq(Row("TQ3280", 5.0)), 532000.0)))
              val (w, h) = size(merged)
              s"${w}x$h"
          },
          "loadable: " + (if (loadable.isEmpty) "none" else loadable.mkString(",")))
        // scalastyle:off println
        lines.foreach(println)
        // scalastyle:on println
    }
}
