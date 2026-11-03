package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.gridx.quadbin.agg.Quadbin_CellUnionAgg
import com.databricks.labs.gbx.gridx.quadbin.generators._
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column

/**
  * GridX Quadbin API entry point: Column helpers for the CARTO quadbin v0 `gbx_quadbin_*` functions
  * (cell math, k-ring, polyfill, tessellate, cellunion, distance).
  *
  * Registration belongs to the Python manager (`databricks.labs.gbx.register(spark)`); this object registers nothing.
  */
object functions extends Serializable {

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

    /** Aggregator: union a group's cells into one geometry (EWKB, EPSG:4326). */
    def quadbin_cellunion_agg(cellid: Column): Column =
        ColumnAdapter(Quadbin_CellUnionAgg.name, Seq(cellid))

    def quadbin_distance(cellid1: Column, cellid2: Column): Column =
        ColumnAdapter(Quadbin_Distance.name, Seq(cellid1, cellid2))

    def quadbin_kloop(cell: Column, k: Column): Column =
        ColumnAdapter(Quadbin_KLoop.name, Seq(cell, k))

    def quadbin_kloop(cell: Column, k: Int): Column =
        quadbin_kloop(cell, lit(k))

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
