package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.util.NodeFileCacheProbe.withScratchDir
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._
import scala.util.Try

/** PixelCombineRasters re-opens the VRT it builds to run the pixel function; that Dataset reads the input
  * files, so it must be closed once the result is translated. */
class PixelCombineRastersTest extends AnyFunSuite {

    /** Paths of the files this JVM holds open (Linux), under `prefix`. */
    private def openFilesUnder(prefix: String): Seq[String] = {
        val fds = Paths.get("/proc/self/fd")
        assume(Files.isDirectory(fds), "needs /proc/self/fd (Linux)")
        val s = Files.list(fds)
        try s.iterator().asScala.flatMap(p => Try(Files.readSymbolicLink(p).toString).toOption).filter(_.startsWith(prefix)).toList
        finally s.close()
    }

    test("combine closes the VRT it re-opens, so no input file stays open after the result is released") {
        GDALManager.init(ExpressionConfig(Map.empty[String, String], new SerializableConfiguration(new Configuration())))
        withScratchDir("gbx_pcr_") { dir =>
            val inputs = (1 to 2).map { i =>
                val p = dir.resolve(s"in$i.tif").toString
                val ds = gdal.GetDriverByName("GTiff").Create(p, 16, 16, 1, gdalconstConstants.GDT_Byte)
                ds.SetGeoTransform(Array(0.0, 1.0, 0.0, 16.0, 0.0, -1.0))
                ds.GetRasterBand(1).Fill(i * 10.0)
                ds.FlushCache()
                ds.delete()
                p
            }
            val dss = inputs.map(p => gdal.Open(p, gdalconstConstants.GA_ReadOnly)).toArray
            val (out, _) =
                try CombineAVG.compute(dss, Map.empty)
                finally dss.foreach(_.delete())
            try {
                assert(out != null)
                val px = new Array[Int](1)
                out.GetRasterBand(1).ReadRaster(0, 0, 1, 1, px)
                withClue("the average of 10 and 20: ") { assert(px(0) == 15) }
            } finally {
                val path = out.GetDescription()
                out.delete()
                gdal.Unlink(path)
            }
            withClue(s"input files held open under $dir after combine: ") { assert(openFilesUnder(dir.toString).isEmpty) }
        }
    }

}
