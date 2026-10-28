package com.databricks.labs.gbx.kernels.udf

import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.kernels.Coerce
import org.apache.spark.sql.api.java.UDF3

import scala.util.control.NonFatal

/** L2 facade for gbx_bng_eastnorthasbng: the same results as L1 (BNG_EastNorthAsBNG). */
class BngEastNorthAsBngUdf extends UDF3[Any, Any, Any, String] {
    override def call(easting: Any, northing: Any, resolution: Any): String = {
        if (easting == null || northing == null || resolution == null) return null
        val res = resolution match {
            case s: String            => BNG.resolutionMap(s) // PARAMETER: an unknown key raises, as L1 does
            case i: java.lang.Integer => i.intValue
            case l: java.lang.Long    => l.intValue
            case o                    => throw new IllegalArgumentException(
              s"gbx_bng_eastnorthasbng: resolution must be INT or STRING, got ${o.getClass.getName}")
        }
        try BNG.format(BNG.pointToCellID(Coerce.double(easting), Coerce.double(northing), res))
        catch { case NonFatal(_) => null } // DATA (NaN, out of range): NULL, as L1's GridErrorHandler
    }
}
