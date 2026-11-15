package com.databricks.labs.gbx.gridx.h3

import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column

/**
  * GridX H3 API entry point: Column helpers for the GeoBrix-native H3 grid functions.
  *
  * H3 primitives (indexing, k-ring, boundaries) are provided by the Databricks product's
  * built-in `h3_*` functions; GeoBrix only adds the H3 grid operations that gap-fill the
  * product surface. Currently that is the `gbx_h3_cellfill` grouped aggregator.
  *
  * Registration belongs to the Python manager (`databricks.labs.gbx.register(spark)`); this object registers nothing.
  */
object functions extends Serializable {

    // ---------- Column API ----------

    /** `gbx_h3_cellfill(cellid, value, k, method, power)` — grouped fill aggregator. */
    def h3_cellfill(cellid: Column, value: Column, k: Column, method: Column, power: Column): Column =
        ColumnAdapter(H3_CellFill.name, Seq(cellid, value, k, method, power))

    def h3_cellfill(cellid: Column, value: Column): Column =
        ColumnAdapter(H3_CellFill.name, Seq(cellid, value))
    def h3_cellfill(cellid: Column, value: Column, k: Int): Column =
        ColumnAdapter(H3_CellFill.name, Seq(cellid, value, lit(k)))
    def h3_cellfill(cellid: Column, value: Column, k: Int, method: String): Column =
        ColumnAdapter(H3_CellFill.name, Seq(cellid, value, lit(k), lit(method)))
    def h3_cellfill(cellid: Column, value: Column, k: Int, method: String, power: Double): Column =
        ColumnAdapter(H3_CellFill.name, Seq(cellid, value, lit(k), lit(method), lit(power)))

}
