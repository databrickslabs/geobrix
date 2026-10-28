package com.databricks.labs.gbx.kernels.udf

import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.kernels.Coerce
import org.apache.spark.sql.api.java.UDF3

import scala.util.control.NonFatal

/** L2 facade for gbx_bng_eastnorthasbng: the same results as L1 (BNG_EastNorthAsBNG). */
class BngEastNorthAsBngUdf extends UDF3[Any, Any, Any, String] {
    override def call(easting: Any, northing: Any, resolution: Any): String = {
        if (easting == null || northing == null || resolution == null) return null
        // PARAMETERS (a resolution BNG does not define, a non-number): raise and name the value, as light does.
        val res = Coerce.bngResolution(resolution, digitStrings = false)
        val (e, n) = (Coerce.double(easting), Coerce.double(northing))
        try BNG.format(BNG.pointToCellID(e, n, res))
        catch { case NonFatal(_) => null } // DATA (NaN, out of range): NULL, as L1's GridErrorHandler
    }
}
