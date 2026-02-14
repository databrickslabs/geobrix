package com.databricks.labs.gbx.vectorx.jts.legacy

import com.databricks.labs.gbx.expressions.RegistryDelegate
import com.databricks.labs.gbx.vectorx.jts.legacy.expressions.ST_LegacyAsWKB
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.{Column, SparkSession}

/** Legacy vector functions: register adds gbx_st_legacyaswkb to the session; st_legacyaswkb(geom) returns WKB column. */
object functions extends Serializable {

    /** Spark conf key used to avoid registering the same functions twice. */
    val flag = "com.databricks.labs.gbx.vectorx.jts.legacy.registered"

    /** Registers gbx_st_legacyaswkb with the session's function registry (idempotent via flag). */
    def register(spark: SparkSession): Unit = {
        val sc = spark.sparkContext
        if (sc.getConf.get(flag, "false") == "true") return // Prevent multiple registrations

        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)
        rd.register(ST_LegacyAsWKB)

        sc.getConf.set(flag, "true")
    }

    /** Returns a Column that invokes gbx_st_legacyaswkb on the given geometry column (legacy internal → WKB). */
    def st_legacyaswkb(geom: Column): Column = ColumnAdapter(ST_LegacyAsWKB.name, Seq(geom))

}
