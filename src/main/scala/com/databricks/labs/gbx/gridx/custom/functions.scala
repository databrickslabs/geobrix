package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.gridx.custom.generators._
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column

/**
  * GridX Custom Grid API entry point: Column helpers for the `gbx_custom_*` functions
  * (grid spec, point-as-cell, cell geometry, k-ring, polyfill, geometry-aware kring/kloop, etc.).
  *
  * Registration belongs to the Python manager (`databricks.labs.gbx.register(spark)`); this object registers nothing.
  */
object functions extends Serializable {

    // ---------- Column API ----------

    // ---------- Scalar / neighbourhood ----------

    def custom_kloop(cell: Column, grid: Column, k: Column): Column =
        ColumnAdapter(Custom_KLoop.name, Seq(cell, grid, k))

    def custom_kloop(cell: Column, grid: Column, k: Int): Column =
        custom_kloop(cell, grid, lit(k))

    def custom_distance(cellid1: Column, grid: Column, cellid2: Column): Column =
        ColumnAdapter(Custom_Distance.name, Seq(cellid1, grid, cellid2))

    def custom_grid(
        boundXMin: Column, boundXMax: Column, boundYMin: Column, boundYMax: Column,
        cellSplits: Column, rootCellSizeX: Column, rootCellSizeY: Column): Column =
        ColumnAdapter(Custom_Grid.name,
            Seq(boundXMin, boundXMax, boundYMin, boundYMax, cellSplits, rootCellSizeX, rootCellSizeY))

    def custom_grid(
        boundXMin: Column, boundXMax: Column, boundYMin: Column, boundYMax: Column,
        cellSplits: Column, rootCellSizeX: Column, rootCellSizeY: Column, srid: Column): Column =
        ColumnAdapter(Custom_Grid.name,
            Seq(boundXMin, boundXMax, boundYMin, boundYMax, cellSplits, rootCellSizeX, rootCellSizeY, srid))

    def custom_pointascell(point: Column, grid: Column, resolution: Column): Column =
        ColumnAdapter(Custom_PointAsCell.name, Seq(point, grid, resolution))

    def custom_pointascell(point: Column, grid: Column, resolution: Int): Column =
        custom_pointascell(point, grid, lit(resolution))

    def custom_cellaswkb(cell: Column, grid: Column): Column =
        ColumnAdapter(Custom_AsWKB.name, Seq(cell, grid))

    def custom_cellaswkt(cell: Column, grid: Column): Column =
        ColumnAdapter(Custom_AsWKT.name, Seq(cell, grid))

    def custom_centroid(cell: Column, grid: Column): Column =
        ColumnAdapter(Custom_Centroid.name, Seq(cell, grid))

    def custom_kring(cell: Column, grid: Column, k: Column): Column =
        ColumnAdapter(Custom_KRing.name, Seq(cell, grid, k))

    def custom_kring(cell: Column, grid: Column, k: Int): Column =
        custom_kring(cell, grid, lit(k))

    def custom_polyfill(geom: Column, grid: Column, resolution: Column): Column =
        ColumnAdapter(Custom_Polyfill.name, Seq(geom, grid, resolution))

    def custom_polyfill(geom: Column, grid: Column, resolution: Int): Column =
        custom_polyfill(geom, grid, lit(resolution))

    // ---------- Aggregators ----------

    def custom_cellfill(cellid: Column, value: Column, grid: Column): Column =
        ColumnAdapter(Custom_CellFill.name, Seq(cellid, value, grid))

    def custom_cellfill(cellid: Column, value: Column, grid: Column, k: Int): Column =
        ColumnAdapter(Custom_CellFill.name, Seq(cellid, value, grid, lit(k)))

    def custom_cellfill(cellid: Column, value: Column, grid: Column, k: Int, method: String): Column =
        ColumnAdapter(Custom_CellFill.name, Seq(cellid, value, grid, lit(k), lit(method)))

    def custom_cellfill(cellid: Column, value: Column, grid: Column, k: Int, method: String, power: Double): Column =
        ColumnAdapter(Custom_CellFill.name, Seq(cellid, value, grid, lit(k), lit(method), lit(power)))

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
