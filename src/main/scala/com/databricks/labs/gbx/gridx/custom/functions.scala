package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.expressions.RegistryDelegate
import com.databricks.labs.gbx.gridx.custom.generators._
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

/**
  * GridX Custom Grid API entry point: register all custom-grid SQL functions.
  *
  * Call `functions.register(spark)` once per session to make `gbx_custom_*` functions available
  * (grid spec, point-as-cell, cell geometry, k-ring, polyfill, geometry-aware kring/kloop, etc.).
  */
object functions extends Serializable {

    val flag = "com.databricks.labs.gbx.gridx.custom.registered"

    /** Register all custom-grid expressions with Spark; idempotent per session. */
    def register(spark: SparkSession): Unit = {
        val sc = spark.sparkContext
        if (sc.getConf.get(flag, "false") == "true") return

        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)

        rd.register(Custom_Grid)
        rd.register(Custom_PointAsCell)
        rd.register(Custom_AsWKB)
        rd.register(Custom_AsWKT)
        rd.register(Custom_Centroid)
        rd.register(Custom_Polyfill)
        rd.register(Custom_KRing)
        rd.register(Custom_KLoop)
        rd.register(Custom_Distance)
        rd.register(Custom_CellFill)
        rd.register(Custom_GeometryKRing)
        rd.register(Custom_GeometryKLoop)

        // Generators
        rd.register(Custom_GeometryKRingExplode)
        rd.register(Custom_GeometryKLoopExplode)

        sc.getConf.set(flag, "true")
    }

    // ---------- Column API ----------

    def custom_geomkring(geom: Column, grid: Column, resolution: Column, k: Column): Column =
        ColumnAdapter(Custom_GeometryKRing.name, Seq(geom, grid, resolution, k))

    def custom_geomkring(geom: Column, grid: Column, resolution: Column, k: Column, mode: Column): Column =
        ColumnAdapter(Custom_GeometryKRing.name, Seq(geom, grid, resolution, k, mode))

    def custom_geomkring(geom: Column, grid: Column, resolution: Column, k: Column, mode: String): Column =
        custom_geomkring(geom, grid, resolution, k, lit(mode))

    def custom_geomkring(geom: Column, grid: Column, resolution: Column, k: Column, mode: Column, coverage: Column): Column =
        ColumnAdapter(Custom_GeometryKRing.name, Seq(geom, grid, resolution, k, mode, coverage))

    def custom_geomkring(geom: Column, grid: Column, resolution: Column, k: Column, mode: String, coverage: String): Column =
        custom_geomkring(geom, grid, resolution, k, lit(mode), lit(coverage))

    def custom_geomkloop(geom: Column, grid: Column, resolution: Column, k: Column): Column =
        ColumnAdapter(Custom_GeometryKLoop.name, Seq(geom, grid, resolution, k))

    def custom_geomkloop(geom: Column, grid: Column, resolution: Column, k: Column, mode: Column): Column =
        ColumnAdapter(Custom_GeometryKLoop.name, Seq(geom, grid, resolution, k, mode))

    def custom_geomkloop(geom: Column, grid: Column, resolution: Column, k: Column, mode: String): Column =
        custom_geomkloop(geom, grid, resolution, k, lit(mode))

    def custom_geomkloop(geom: Column, grid: Column, resolution: Column, k: Column, mode: Column, coverage: Column): Column =
        ColumnAdapter(Custom_GeometryKLoop.name, Seq(geom, grid, resolution, k, mode, coverage))

    def custom_geomkloop(geom: Column, grid: Column, resolution: Column, k: Column, mode: String, coverage: String): Column =
        custom_geomkloop(geom, grid, resolution, k, lit(mode), lit(coverage))

    // Generators
    def custom_geomkringexplode(geom: Column, grid: Column, resolution: Column, k: Column): Column =
        ColumnAdapter(Custom_GeometryKRingExplode.name, Seq(geom, grid, resolution, k))

    def custom_geomkringexplode(geom: Column, grid: Column, resolution: Column, k: Column, mode: Column): Column =
        ColumnAdapter(Custom_GeometryKRingExplode.name, Seq(geom, grid, resolution, k, mode))

    def custom_geomkringexplode(geom: Column, grid: Column, resolution: Column, k: Column, mode: Column, coverage: Column): Column =
        ColumnAdapter(Custom_GeometryKRingExplode.name, Seq(geom, grid, resolution, k, mode, coverage))

    def custom_geomkloopexplode(geom: Column, grid: Column, resolution: Column, k: Column): Column =
        ColumnAdapter(Custom_GeometryKLoopExplode.name, Seq(geom, grid, resolution, k))

    def custom_geomkloopexplode(geom: Column, grid: Column, resolution: Column, k: Column, mode: Column): Column =
        ColumnAdapter(Custom_GeometryKLoopExplode.name, Seq(geom, grid, resolution, k, mode))

    def custom_geomkloopexplode(geom: Column, grid: Column, resolution: Column, k: Column, mode: Column, coverage: Column): Column =
        ColumnAdapter(Custom_GeometryKLoopExplode.name, Seq(geom, grid, resolution, k, mode, coverage))

    // ---------- Scalar-literal overloads ----------

    def custom_geomkring(geom: Column, grid: Column, resolution: Int, k: Int): Column =
        custom_geomkring(geom, grid, lit(resolution), lit(k))

    def custom_geomkring(geom: Column, grid: Column, resolution: Int, k: Int, mode: String): Column =
        custom_geomkring(geom, grid, lit(resolution), lit(k), lit(mode))

    def custom_geomkring(geom: Column, grid: Column, resolution: Int, k: Int, mode: String, coverage: String): Column =
        custom_geomkring(geom, grid, lit(resolution), lit(k), lit(mode), lit(coverage))

    def custom_geomkloop(geom: Column, grid: Column, resolution: Int, k: Int): Column =
        custom_geomkloop(geom, grid, lit(resolution), lit(k))

    def custom_geomkloop(geom: Column, grid: Column, resolution: Int, k: Int, mode: String): Column =
        custom_geomkloop(geom, grid, lit(resolution), lit(k), lit(mode))

    def custom_geomkloop(geom: Column, grid: Column, resolution: Int, k: Int, mode: String, coverage: String): Column =
        custom_geomkloop(geom, grid, lit(resolution), lit(k), lit(mode), lit(coverage))

    def custom_geomkringexplode(geom: Column, grid: Column, resolution: Int, k: Int): Column =
        custom_geomkringexplode(geom, grid, lit(resolution), lit(k))

    def custom_geomkringexplode(geom: Column, grid: Column, resolution: Int, k: Int, mode: String): Column =
        custom_geomkringexplode(geom, grid, lit(resolution), lit(k), lit(mode))

    def custom_geomkringexplode(geom: Column, grid: Column, resolution: Int, k: Int, mode: String, coverage: String): Column =
        custom_geomkringexplode(geom, grid, lit(resolution), lit(k), lit(mode), lit(coverage))

    def custom_geomkloopexplode(geom: Column, grid: Column, resolution: Int, k: Int): Column =
        custom_geomkloopexplode(geom, grid, lit(resolution), lit(k))

    def custom_geomkloopexplode(geom: Column, grid: Column, resolution: Int, k: Int, mode: String): Column =
        custom_geomkloopexplode(geom, grid, lit(resolution), lit(k), lit(mode))

    def custom_geomkloopexplode(geom: Column, grid: Column, resolution: Int, k: Int, mode: String, coverage: String): Column =
        custom_geomkloopexplode(geom, grid, lit(resolution), lit(k), lit(mode), lit(coverage))

}
