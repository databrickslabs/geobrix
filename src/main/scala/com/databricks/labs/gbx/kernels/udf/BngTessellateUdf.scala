package com.databricks.labs.gbx.kernels.udf

import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.kernels.Coerce
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF3

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** L2 facade for gbx_bng_tessellate: the same chips as L1 (BNG_Tessellate), as `array<struct<cellid, core, chip>>`.
  * A Java UDF has a fixed arity, so the third argument (keep_core_geom) is always passed. */
class BngTessellateUdf extends UDF3[Any, Any, Any, java.util.List[Row]] {
    override def call(geom: Any, resolution: Any, keepCore: Any): java.util.List[Row] = {
        if (geom == null || resolution == null) return null
        // PARAMETER: a resolution BNG does not define raises, naming the value, as on every other level.
        val res = Coerce.bngResolution(resolution, digitStrings = false) // an INT index or a size name, as L1
        val keep = keepCore match { case b: java.lang.Boolean => b.booleanValue; case _ => false }
        try {
            val g = geom match { case b: Array[Byte] => JTS.fromWKB(b); case s: String => JTS.fromWKT(s) }
            BNG.tessellate(g, res, keep).map { case (id, core, chip) =>
                Row(BNG.format(id), core, if (chip == null) null else JTS.toWKB(chip)) }.toSeq.asJava
        } catch { case NonFatal(_) => null } // DATA (a geometry that cannot be read or tessellated): NULL, as L1
    }
}
