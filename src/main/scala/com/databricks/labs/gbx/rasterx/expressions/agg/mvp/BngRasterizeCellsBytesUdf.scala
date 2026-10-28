package com.databricks.labs.gbx.rasterx.expressions.agg.mvp

import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.kernels.{Coerce, KernelGdal}
import com.databricks.labs.gbx.rasterx.expressions.agg.RasterizeBurn
import com.databricks.labs.gbx.rasterx.util.VectorRasterBridge
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF7

/** L2 finalize for collect_list(struct(cellid, value)) on an explicit canvas: RST_BNG_RasterizeAgg.eval
  * without its Catalyst buffer. The duplicate rule is the same (it is the same code, [[RasterizeBurn.canonicalLut]]):
  * canonical (cellId, value) order, so the largest value wins. Nothing here may need a Catalyst class: this runs in a
  * Java UDF on a standard cluster. A NULL value counts as 1.0, as in the aggregate (the lightweight tier burns NaN
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
        val res = RasterizeBurn.bngResolutionOf(pairs.iterator.map(_._1))
        val lut = RasterizeBurn.canonicalLut(pairs)
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
