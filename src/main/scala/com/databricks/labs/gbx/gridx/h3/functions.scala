package com.databricks.labs.gbx.gridx.h3

import com.databricks.labs.gbx.expressions.RegistryDelegate
import com.databricks.labs.gbx.rasterx.expressions.grid.RST_H3_CellBBox
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

/**
  * GridX H3 API entry point: register GeoBrix-native H3 grid SQL functions.
  *
  * H3 primitives (indexing, k-ring, boundaries) are provided by the Databricks product's
  * built-in `h3_*` functions; GeoBrix only adds the H3 grid operations that gap-fill the
  * product surface. Currently that is the `gbx_h3_cellfill` grouped aggregator.
  *
  * Call `functions.register(spark)` once per session to make `gbx_h3_*` functions available.
  */
object functions extends Serializable {

    val flag = "com.databricks.labs.gbx.gridx.h3.registered"

    /** Register all GeoBrix H3 grid expressions with Spark; idempotent per session. */
    def register(spark: SparkSession): Unit = {
        val sc = spark.sparkContext
        if (sc.getConf.get(flag, "false") == "true") return

        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)

        // Aggregators
        rd.register(H3_CellFill)

        sc.getConf.set(flag, "true")
    }

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

    /** `gbx_h3_cell_bbox(cellid, srid, mode, kring_pad, out_crs)`: the bounding box of an H3 cell. */
    def h3_cell_bbox(cellid: Column): Column =
        ColumnAdapter(RST_H3_CellBBox.name, Seq(cellid))
    def h3_cell_bbox(cellid: Column, srid: Column): Column =
        ColumnAdapter(RST_H3_CellBBox.name, Seq(cellid, srid))
    def h3_cell_bbox(cellid: Column, srid: Column, mode: Column): Column =
        ColumnAdapter(RST_H3_CellBBox.name, Seq(cellid, srid, mode))
    def h3_cell_bbox(cellid: Column, srid: Column, mode: Column, kringPad: Column): Column =
        ColumnAdapter(RST_H3_CellBBox.name, Seq(cellid, srid, mode, kringPad))
    def h3_cell_bbox(cellid: Column, srid: Column, mode: Column, kringPad: Column, outCrs: Column): Column =
        ColumnAdapter(RST_H3_CellBBox.name, Seq(cellid, srid, mode, kringPad, outCrs))
}
