package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.expressions.RegistryDelegate
import com.databricks.labs.gbx.gridx.quadbin.agg.Quadbin_CellUnionAgg
import com.databricks.labs.gbx.gridx.quadbin.generators._
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
        rd.register(Quadbin_KLoop)
        rd.register(Quadbin_Tessellate)
        rd.register(Quadbin_CellUnion)
        rd.register(Quadbin_CellUnionAgg)
        rd.register(Quadbin_CellFill)
        rd.register(Quadbin_Distance)
        rd.register(Quadbin_GeometryKRing)
        rd.register(Quadbin_GeometryKLoop)

        // Generators
        rd.register(Quadbin_GeometryKRingExplode)
        rd.register(Quadbin_GeometryKLoopExplode)

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

    def quadbin_cellfill(cellid: Column, value: Column): Column =
        ColumnAdapter(Quadbin_CellFill.name, Seq(cellid, value))
    def quadbin_cellfill(cellid: Column, value: Column, k: Int): Column =
        ColumnAdapter(Quadbin_CellFill.name, Seq(cellid, value, lit(k)))
    def quadbin_cellfill(cellid: Column, value: Column, k: Int, method: String): Column =
        ColumnAdapter(Quadbin_CellFill.name, Seq(cellid, value, lit(k), lit(method)))
    def quadbin_cellfill(cellid: Column, value: Column, k: Int, method: String, power: Double): Column =
        ColumnAdapter(Quadbin_CellFill.name, Seq(cellid, value, lit(k), lit(method), lit(power)))

    def quadbin_geomkring(geom: Column, resolution: Column, k: Column): Column =
        ColumnAdapter(Quadbin_GeometryKRing.name, Seq(geom, resolution, k))

    def quadbin_geomkring(geom: Column, resolution: Column, k: Column, mode: Column): Column =
        ColumnAdapter(Quadbin_GeometryKRing.name, Seq(geom, resolution, k, mode))

    def quadbin_geomkring(geom: Column, resolution: Column, k: Column, mode: String): Column =
        quadbin_geomkring(geom, resolution, k, lit(mode))

    def quadbin_geomkring(geom: Column, resolution: Column, k: Column, mode: Column, coverage: Column): Column =
        ColumnAdapter(Quadbin_GeometryKRing.name, Seq(geom, resolution, k, mode, coverage))

    def quadbin_geomkring(geom: Column, resolution: Column, k: Column, mode: String, coverage: String): Column =
        quadbin_geomkring(geom, resolution, k, lit(mode), lit(coverage))

    def quadbin_geomkloop(geom: Column, resolution: Column, k: Column): Column =
        ColumnAdapter(Quadbin_GeometryKLoop.name, Seq(geom, resolution, k))

    def quadbin_geomkloop(geom: Column, resolution: Column, k: Column, mode: Column): Column =
        ColumnAdapter(Quadbin_GeometryKLoop.name, Seq(geom, resolution, k, mode))

    def quadbin_geomkloop(geom: Column, resolution: Column, k: Column, mode: String): Column =
        quadbin_geomkloop(geom, resolution, k, lit(mode))

    def quadbin_geomkloop(geom: Column, resolution: Column, k: Column, mode: Column, coverage: Column): Column =
        ColumnAdapter(Quadbin_GeometryKLoop.name, Seq(geom, resolution, k, mode, coverage))

    def quadbin_geomkloop(geom: Column, resolution: Column, k: Column, mode: String, coverage: String): Column =
        quadbin_geomkloop(geom, resolution, k, lit(mode), lit(coverage))

    // Generators
    def quadbin_geomkringexplode(geom: Column, resolution: Column, k: Column): Column =
        ColumnAdapter(Quadbin_GeometryKRingExplode.name, Seq(geom, resolution, k))

    def quadbin_geomkringexplode(geom: Column, resolution: Column, k: Column, mode: Column): Column =
        ColumnAdapter(Quadbin_GeometryKRingExplode.name, Seq(geom, resolution, k, mode))

    def quadbin_geomkringexplode(geom: Column, resolution: Column, k: Column, mode: Column, coverage: Column): Column =
        ColumnAdapter(Quadbin_GeometryKRingExplode.name, Seq(geom, resolution, k, mode, coverage))

    def quadbin_geomkloopexplode(geom: Column, resolution: Column, k: Column): Column =
        ColumnAdapter(Quadbin_GeometryKLoopExplode.name, Seq(geom, resolution, k))

    def quadbin_geomkloopexplode(geom: Column, resolution: Column, k: Column, mode: Column): Column =
        ColumnAdapter(Quadbin_GeometryKLoopExplode.name, Seq(geom, resolution, k, mode))

    def quadbin_geomkloopexplode(geom: Column, resolution: Column, k: Column, mode: Column, coverage: Column): Column =
        ColumnAdapter(Quadbin_GeometryKLoopExplode.name, Seq(geom, resolution, k, mode, coverage))

    // ---------- Scalar-literal overloads ----------

    def quadbin_pointascell(longitude: Column, latitude: Column, resolution: Int): Column =
        quadbin_pointascell(longitude, latitude, lit(resolution))

    def quadbin_polyfill(geom: Column, resolution: Int): Column =
        quadbin_polyfill(geom, lit(resolution))

    def quadbin_kring(cell: Column, k: Int): Column =
        quadbin_kring(cell, lit(k))

    def quadbin_tessellate(geom: Column, resolution: Int): Column =
        quadbin_tessellate(geom, lit(resolution))

    def quadbin_geomkring(geom: Column, resolution: Int, k: Int): Column =
        quadbin_geomkring(geom, lit(resolution), lit(k))

    def quadbin_geomkring(geom: Column, resolution: Int, k: Int, mode: String): Column =
        quadbin_geomkring(geom, lit(resolution), lit(k), lit(mode))

    def quadbin_geomkring(geom: Column, resolution: Int, k: Int, mode: String, coverage: String): Column =
        quadbin_geomkring(geom, lit(resolution), lit(k), lit(mode), lit(coverage))

    def quadbin_geomkloop(geom: Column, resolution: Int, k: Int): Column =
        quadbin_geomkloop(geom, lit(resolution), lit(k))

    def quadbin_geomkloop(geom: Column, resolution: Int, k: Int, mode: String): Column =
        quadbin_geomkloop(geom, lit(resolution), lit(k), lit(mode))

    def quadbin_geomkloop(geom: Column, resolution: Int, k: Int, mode: String, coverage: String): Column =
        quadbin_geomkloop(geom, lit(resolution), lit(k), lit(mode), lit(coverage))

    def quadbin_geomkringexplode(geom: Column, resolution: Int, k: Int): Column =
        quadbin_geomkringexplode(geom, lit(resolution), lit(k))

    def quadbin_geomkringexplode(geom: Column, resolution: Int, k: Int, mode: String): Column =
        quadbin_geomkringexplode(geom, lit(resolution), lit(k), lit(mode))

    def quadbin_geomkringexplode(geom: Column, resolution: Int, k: Int, mode: String, coverage: String): Column =
        quadbin_geomkringexplode(geom, lit(resolution), lit(k), lit(mode), lit(coverage))

    def quadbin_geomkloopexplode(geom: Column, resolution: Int, k: Int): Column =
        quadbin_geomkloopexplode(geom, lit(resolution), lit(k))

    def quadbin_geomkloopexplode(geom: Column, resolution: Int, k: Int, mode: String): Column =
        quadbin_geomkloopexplode(geom, lit(resolution), lit(k), lit(mode))

    def quadbin_geomkloopexplode(geom: Column, resolution: Int, k: Int, mode: String, coverage: String): Column =
        quadbin_geomkloopexplode(geom, lit(resolution), lit(k), lit(mode), lit(coverage))

}
