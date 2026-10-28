package com.databricks.labs.gbx.kernels.df

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.expressions.analysis.RST_Viewshed
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.RST_ExpressionUtil
import com.databricks.labs.gbx.util.{NodeFileLease, NodeFileManager}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.gdal.gdal.{BuildVRTOptions, Dataset, gdal}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

case class TowerIn(tower_id: String, observer_x: Double, observer_y: Double, observer_height: Double)
case class TowerOut(tower_id: String, observer_x: Double, observer_y: Double, visible_px: Long, total_px: Long)

/** `rst_viewshed_towers` as ONE Scala `mapPartitions` (dedicated clusters; the Python helper calls it over py4j).
  * Per tower: pick the DSM tiles in reach, mosaic them in a VRT, run the viewshed and count the visible and the
  * invisible pixels. Executors read the tiles through NodeFileManager (Hadoop FS and the UC connector, so a bare
  * /Volumes path works); a tower with no tile in reach has no row. */
object ViewshedTowersJvm {

    /** `towers` has tower_id, observer_x, observer_y and observer_height; `tiles` maps "tx,ty" to a tile path. */
    def run(spark: SparkSession, towers: DataFrame, tiles: java.util.Map[String, String], x0: Double, y0: Double,
            tileM: Double, radius: Double, targetHeight: Double, numPartitions: Int): DataFrame = {
        import spark.implicits._
        val conf = ExpressionConfig(spark)
        val tileMap = tiles.asScala.toMap
        towers.as[TowerIn].repartition(numPartitions, col("tower_id"))
            .mapPartitions { it => RST_ExpressionUtil.init(conf); it.flatMap(one(_, tileMap, x0, y0, tileM, radius, targetHeight)) }
            .toDF()
    }

    private def span(c: Double, o: Double, r: Double, t: Double): Range =
        math.floor((c - r - o) / t).toInt to math.floor((c + r - o) / t).toInt // == pyrx tiling.tile_range

    private def one(t: TowerIn, tiles: Map[String, String], x0: Double, y0: Double, tileM: Double, radius: Double,
                    targetHeight: Double): Option[TowerOut] = {
        val paths = (for (tx <- span(t.observer_x, x0, radius, tileM); ty <- span(t.observer_y, y0, radius, tileM);
                          p <- tiles.get(s"$tx,$ty")) yield p).distinct.sorted
        if (paths.isEmpty) return None
        val leases = ArrayBuffer.empty[NodeFileLease]
        try {
            paths.foreach(p => leases += NodeFileManager.acquire(p))
            val vrtPath = s"/vsimem/towers_${java.util.UUID.randomUUID().toString.replace("-", "")}.vrt"
            val sources = new java.util.Vector[String](leases.map(_.localPath).asJava)
            val vrt = gdal.BuildVRT(vrtPath, sources, new BuildVRTOptions(new java.util.Vector[String]()))
            if (vrt == null) throw new RuntimeException(s"rst_viewshed_towers: no VRT over ${paths.mkString(", ")}: ${gdal.GetLastErrorMsg}")
            try {
                val (vs, _) = RST_Viewshed.execute(vrt, Map.empty, t.observer_x, t.observer_y, t.observer_height,
                                                   targetHeight, Some(radius))
                try {
                    val (vis, tot) = count(vs)
                    Some(TowerOut(t.tower_id, t.observer_x, t.observer_y, vis, tot))
                } finally RasterDriver.releaseDataset(vs)
            } finally {
                // Not releaseDataset: it would give back the leases' holds on the tiles a second time.
                vrt.delete()
                gdal.Unlink(vrtPath)
            }
        } finally leases.foreach(_.close())
    }

    /** (visible, visible + invisible) of a binary viewshed: 255 is visible, 0 is not; nothing else is counted. */
    private def count(vs: Dataset): (Long, Long) = {
        val (w, h) = (vs.GetRasterXSize, vs.GetRasterYSize)
        val band = vs.GetRasterBand(1)
        val buf = new Array[Byte](w)
        var vis, tot = 0L
        for (row <- 0 until h) {
            band.ReadRaster(0, row, w, 1, buf)
            buf.foreach { v => val u = v & 0xff; if (u == 255) { vis += 1; tot += 1 } else if (u == 0) tot += 1 }
        }
        (vis, tot)
    }
}
