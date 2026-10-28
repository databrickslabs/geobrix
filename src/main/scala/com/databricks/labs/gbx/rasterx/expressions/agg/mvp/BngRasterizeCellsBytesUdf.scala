package com.databricks.labs.gbx.rasterx.expressions.agg.mvp

import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.kernels.{Coerce, KernelGdal}
import com.databricks.labs.gbx.rasterx.expressions.agg.{RST_BNG_RasterizeAgg, RasterizeBurn}
import com.databricks.labs.gbx.rasterx.util.VectorRasterBridge
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF7

/** L2 finalize for collect_list(struct(cellid, value)) on an explicit canvas: RST_BNG_RasterizeAgg.eval
  * (:192-262) without its Catalyst buffer. The duplicate rule is the same: canonical (cellId, value) order,
  * so the largest value wins. A NULL value counts as 1.0, as in the aggregate (the lightweight tier burns NaN
  * for it: raster-functions.mdx says so). NULL when no row has a cell id. */
class BngRasterizeCellsBytesUdf extends UDF7[Any, Any, Any, Any, Any, Any, Any, Array[Byte]] {
    override def call(cells: Any, xmin: Any, ymin: Any, xmax: Any, ymax: Any, width: Any, height: Any): Array[Byte] = {
        val pairs = cells match {
            case s: scala.collection.Seq[_] => s.collect {
                case r: Row if r != null && !r.isNullAt(0) =>
                    (BNG.parse(r.getString(0)), if (r.isNullAt(1)) 1.0 else r.getDouble(1))
            }
            case _ => Seq.empty[(Long, Double)]
        }
        if (pairs.isEmpty) return null
        val res = RST_BNG_RasterizeAgg.resolutionOf(pairs.iterator.map(_._1))
        val lut = scala.collection.mutable.LongMap.empty[Double]
        pairs.sortWith((a, b) => if (a._1 != b._1) java.lang.Long.compareUnsigned(a._1, b._1) < 0 else a._2 < b._2)
            .foreach { case (c, v) => lut.update(c, v) }
        KernelGdal.ensureInit()
        val (w, h) = (Coerce.double(width).toInt, Coerce.double(height).toInt)
        val ds = VectorRasterBridge.buildEmptyRaster(Coerce.double(xmin), Coerce.double(ymin), Coerce.double(xmax),
                                                     Coerce.double(ymax), w, h, BNG.crsID)
        try {
            RasterizeBurn.burn(BNG, lut, BNG.crsID, res, ds, w, h)
            ds.FlushCache()
            VectorRasterBridge.toGTiffBytes(ds)
        } finally ds.delete()
    }
}
