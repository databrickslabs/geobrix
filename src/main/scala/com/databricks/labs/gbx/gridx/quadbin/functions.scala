package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.expressions.RegistryDelegate
import com.databricks.labs.gbx.gridx.quadbin.agg.Quadbin_CellUnionAgg
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

/**
  * GridX Quadbin API entry point: register all CARTO quadbin v0 SQL functions.
  *
  * Call `functions.register(spark)` once per session to make `gbx_quadbin_*`
  * functions available (cell math, k-ring, polyfill, tessellate, cellunion, distance).
  */
object functions extends Serializable {

    val flag = "com.databricks.labs.gbx.gridx.quadbin.registered"

    /** Register all Quadbin expressions with Spark; idempotent per session. */
    def register(spark: SparkSession): Unit = {
        val sc = spark.sparkContext
        if (sc.getConf.get(flag, "false") == "true") return

        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)

        rd.register(Quadbin_PointAsCell)
        rd.register(Quadbin_AsWKB)
        rd.register(Quadbin_Centroid)
        rd.register(Quadbin_Resolution)
        rd.register(Quadbin_Polyfill)
        rd.register(Quadbin_KRing)
        rd.register(Quadbin_Tessellate)
        rd.register(Quadbin_CellUnion)
        rd.register(Quadbin_CellUnionAgg)
        rd.register(Quadbin_Distance)

        sc.getConf.set(flag, "true")
    }

    // ---------- Column API ----------

    def quadbin_pointascell(longitude: Column, latitude: Column, resolution: Column): Column =
        ColumnAdapter(Quadbin_PointAsCell.name, Seq(longitude, latitude, resolution))

    def quadbin_aswkb(cell: Column): Column = ColumnAdapter(Quadbin_AsWKB.name, Seq(cell))

    def quadbin_centroid(cell: Column): Column = ColumnAdapter(Quadbin_Centroid.name, Seq(cell))

    def quadbin_resolution(cell: Column): Column = ColumnAdapter(Quadbin_Resolution.name, Seq(cell))

    def quadbin_polyfill(geom: Column, resolution: Column): Column =
        ColumnAdapter(Quadbin_Polyfill.name, Seq(geom, resolution))

    def quadbin_kring(cell: Column, k: Column): Column =
        ColumnAdapter(Quadbin_KRing.name, Seq(cell, k))

    def quadbin_tessellate(geom: Column, resolution: Column): Column =
        ColumnAdapter(Quadbin_Tessellate.name, Seq(geom, resolution))

    def quadbin_cellunion(cells: Column): Column =
        ColumnAdapter(Quadbin_CellUnion.name, Seq(cells))

    def quadbin_distance(cellA: Column, cellB: Column): Column =
        ColumnAdapter(Quadbin_Distance.name, Seq(cellA, cellB))

    // ---------- Scalar-literal overloads ----------

    def quadbin_pointascell(longitude: Column, latitude: Column, resolution: Int): Column =
        quadbin_pointascell(longitude, latitude, lit(resolution))

    def quadbin_polyfill(geom: Column, resolution: Int): Column =
        quadbin_polyfill(geom, lit(resolution))

    def quadbin_kring(cell: Column, k: Int): Column =
        quadbin_kring(cell, lit(k))

    def quadbin_tessellate(geom: Column, resolution: Int): Column =
        quadbin_tessellate(geom, lit(resolution))

}
