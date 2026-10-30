package com.databricks.labs.gbx.pmtiles

import com.databricks.labs.gbx.expressions.RegistryDelegate
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

/**
  * PMTiles API entry point: register the PMTiles SQL UDAF and expose Scala API helpers.
  *
  * Call `functions.register(spark)` once per session to make the `gbx_pmtiles_agg`
  * SQL function available. The `.write.format("pmtiles")` DataSource writer is
  * registered automatically via `META-INF/services/...DataSourceRegister`.
  *
  * Naming: SQL `gbx_pmtiles_agg` → Scala `pmtiles_agg` → Python `pmtiles_agg`
  * (single canonical name; no aliases).
  */
object functions extends Serializable {

    val flag = "com.databricks.labs.gbx.pmtiles.registered"

    /** Register PMTiles expressions with Spark; idempotent per session. */
    def register(spark: SparkSession): Unit = {
        val sc = spark.sparkContext
        if (sc.getConf.get(flag, "false") == "true") return

        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)

        rd.register(PMTiles_Agg)

        sc.getConf.set(flag, "true")
    }

    /**
      * Scala API: aggregate tile rows into a single PMTile v3 BINARY blob.
      *
      * @param bytes  Tile-payload column (BINARY) — passed through verbatim.
      * @param z      Tile zoom column (INT).
      * @param x      Tile x column (INT).
      * @param y      Tile y column (INT).
      * @param metadataJson Optional JSON metadata column (STRING); defaults to `"{}"`.
      */
    def pmtiles_agg(bytes: Column, z: Column, x: Column, y: Column, metadataJson: Column): Column =
        ColumnAdapter(PMTiles_Agg.name, Seq(bytes, z, x, y, metadataJson))

    /** 4-arg overload — metadata defaults to `"{}"`. */
    def pmtiles_agg(bytes: Column, z: Column, x: Column, y: Column): Column =
        ColumnAdapter(PMTiles_Agg.name, Seq(bytes, z, x, y, lit("{}")))

    /** Scala-friendly overload: pass a plain JSON string literal as metadata. */
    def pmtiles_agg(bytes: Column, z: Column, x: Column, y: Column, metadataJson: String): Column =
        pmtiles_agg(bytes, z, x, y, lit(metadataJson))
}
