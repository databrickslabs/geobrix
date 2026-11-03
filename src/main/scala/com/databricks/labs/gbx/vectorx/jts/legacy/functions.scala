package com.databricks.labs.gbx.vectorx.jts.legacy

import com.databricks.labs.gbx.vectorx.jts.legacy.expressions.ST_LegacyAsWKB
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.Column

/** Legacy vector functions: st_legacyaswkb(geom) returns a WKB column. Registration belongs to the Python manager. */
object functions extends Serializable {

    /** Returns a Column that invokes gbx_st_legacyaswkb on the given geometry column (legacy internal -> WKB). */
    def st_legacyaswkb(geom: Column): Column = ColumnAdapter(ST_LegacyAsWKB.name, Seq(geom))

}
