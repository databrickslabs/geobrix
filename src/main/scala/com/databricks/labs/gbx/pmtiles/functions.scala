package com.databricks.labs.gbx.pmtiles

import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column

/**
  * PMTiles API entry point: Scala API helpers for the `gbx_pmtiles_agg` SQL UDAF.
  *
  * Registration belongs to the Python manager (`databricks.labs.gbx.register(spark)`); this object registers nothing.
  *
  * Naming: SQL `gbx_pmtiles_agg` → Scala `pmtiles_agg` → Python `pmtiles_agg`
  * (single canonical name; no aliases).
  */
object functions extends Serializable {

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
