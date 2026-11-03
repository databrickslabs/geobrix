package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.gridx.bng.agg.{BNG_CellIntersectionAgg, BNG_CellUnionAgg}
import com.databricks.labs.gbx.gridx.bng.generators._
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column

/**
  * GridX BNG API entry point: Column helpers for the `gbx_bng_*` (British National Grid) functions
  * (cell geometry, k-ring, polyfill, tessellate, aggregations, etc.).
  *
  * Registration belongs to the Python manager (`databricks.labs.gbx.register(spark)`); this object registers nothing.
  */
object functions extends Serializable {

    def bng_aswkb(cellid: Column): Column = ColumnAdapter(BNG_AsWKB.name, Seq(cellid))
    def bng_aswkt(cellid: Column): Column = ColumnAdapter(BNG_AsWKT.name, Seq(cellid))
    def bng_cellarea(cellid: Column): Column = ColumnAdapter(BNG_CellArea.name, Seq(cellid))
    def bng_cellintersection(leftChip: Column, rightChip: Column): Column = ColumnAdapter(BNG_CellIntersection.name, Seq(leftChip, rightChip))
    def bng_cellunion(leftChip: Column, rightChip: Column): Column = ColumnAdapter(BNG_CellUnion.name, Seq(leftChip, rightChip))
    def bng_centroid(cellid: Column): Column = ColumnAdapter(BNG_Centroid.name, Seq(cellid))
    def bng_distance(cellid1: Column, cellid2: Column): Column = ColumnAdapter(BNG_Distance.name, Seq(cellid1, cellid2))
    def bng_eastnorthasbng(easting: Column, northing: Column, resolution: Column): Column =
        ColumnAdapter(BNG_EastNorthAsBNG.name, Seq(easting, northing, resolution))
    def bng_euclideandistance(cellid1: Column, cellid2: Column): Column = ColumnAdapter(BNG_EuclideanDistance.name, Seq(cellid1, cellid2))
    def bng_geomkloop(geom: Column, resolution: Column, k: Column): Column = ColumnAdapter(BNG_GeometryKLoop.name, Seq(geom, resolution, k))
    def bng_geomkloop(geom: Column, resolution: Column, k: Column, mode: Column): Column = ColumnAdapter(BNG_GeometryKLoop.name, Seq(geom, resolution, k, mode))
    def bng_geomkloop(geom: Column, resolution: Column, k: Column, mode: Column, coverage: Column): Column = ColumnAdapter(BNG_GeometryKLoop.name, Seq(geom, resolution, k, mode, coverage))
    def bng_geomkring(geom: Column, resolution: Column, k: Column): Column = ColumnAdapter(BNG_GeometryKRing.name, Seq(geom, resolution, k))
    def bng_geomkring(geom: Column, resolution: Column, k: Column, mode: Column): Column = ColumnAdapter(BNG_GeometryKRing.name, Seq(geom, resolution, k, mode))
    def bng_geomkring(geom: Column, resolution: Column, k: Column, mode: Column, coverage: Column): Column = ColumnAdapter(BNG_GeometryKRing.name, Seq(geom, resolution, k, mode, coverage))
    def bng_kloop(cellid: Column, k: Column): Column = ColumnAdapter(BNG_KLoop.name, Seq(cellid, k))
    def bng_kring(cellid: Column, k: Column): Column = ColumnAdapter(BNG_KRing.name, Seq(cellid, k))
    def bng_pointascell(geom: Column, resolution: Column): Column = ColumnAdapter(BNG_PointAsCell.name, Seq(geom, resolution))
    def bng_polyfill(geom: Column, resolution: Column): Column = ColumnAdapter(BNG_Polyfill.name, Seq(geom, resolution))
    /** `keepCoreGeom` defaults to false: a core cell's chip is NULL, as in SQL's two-argument form and the lightweight tier. */
    def bng_tessellate(geom: Column, resolution: Column): Column = bng_tessellate(geom, resolution, lit(false))
    def bng_tessellate(geom: Column, resolution: Column, keepCoreGeom: Column): Column =
        ColumnAdapter(BNG_Tessellate.name, Seq(geom, resolution, keepCoreGeom))

    // Aggregators
    def bng_cellfill(cellid: Column, value: Column): Column =
        ColumnAdapter(BNG_CellFill.name, Seq(cellid, value))
    def bng_cellfill(cellid: Column, value: Column, k: Int): Column =
        ColumnAdapter(BNG_CellFill.name, Seq(cellid, value, lit(k)))
    def bng_cellfill(cellid: Column, value: Column, k: Int, method: String): Column =
        ColumnAdapter(BNG_CellFill.name, Seq(cellid, value, lit(k), lit(method)))
    def bng_cellfill(cellid: Column, value: Column, k: Int, method: String, power: Double): Column =
        ColumnAdapter(BNG_CellFill.name, Seq(cellid, value, lit(k), lit(method), lit(power)))

    // Generators
    def bng_geomkloopexplode(geom: Column, resolution: Column, k: Column): Column =
        ColumnAdapter(BNG_GeometryKLoopExplode.name, Seq(geom, resolution, k))
    def bng_geomkloopexplode(geom: Column, resolution: Column, k: Column, mode: Column): Column =
        ColumnAdapter(BNG_GeometryKLoopExplode.name, Seq(geom, resolution, k, mode))
    def bng_geomkloopexplode(geom: Column, resolution: Column, k: Column, mode: Column, coverage: Column): Column =
        ColumnAdapter(BNG_GeometryKLoopExplode.name, Seq(geom, resolution, k, mode, coverage))
    def bng_geomkringexplode(geom: Column, resolution: Column, k: Column): Column =
        ColumnAdapter(BNG_GeometryKRingExplode.name, Seq(geom, resolution, k))
    def bng_geomkringexplode(geom: Column, resolution: Column, k: Column, mode: Column): Column =
        ColumnAdapter(BNG_GeometryKRingExplode.name, Seq(geom, resolution, k, mode))
    def bng_geomkringexplode(geom: Column, resolution: Column, k: Column, mode: Column, coverage: Column): Column =
        ColumnAdapter(BNG_GeometryKRingExplode.name, Seq(geom, resolution, k, mode, coverage))
    def bng_kloopexplode(cellid: Column, k: Column): Column = ColumnAdapter(BNG_KLoopExplode.name, Seq(cellid, k))
    def bng_kringexplode(cellid: Column, k: Column): Column = ColumnAdapter(BNG_KRingExplode.name, Seq(cellid, k))
    /** `keepCoreGeom` defaults to false: a core cell's chip is NULL, as in the lightweight tier. */
    def bng_tessellateexplode(geom: Column, resolution: Column): Column = bng_tessellateexplode(geom, resolution, lit(false))
    def bng_tessellateexplode(geom: Column, resolution: Column, keepCoreGeom: Column): Column =
        ColumnAdapter(BNG_TessellateExplode.name, Seq(geom, resolution, keepCoreGeom))

    // Scalar-literal overloads — so users can pass plain values for non-Column params.
    // Resolution accepts Int (±1..±6 index) or String ("1km", "100m"). k is Int.
    def bng_eastnorthasbng(easting: Column, northing: Column, resolution: Int): Column =
        bng_eastnorthasbng(easting, northing, lit(resolution))
    def bng_eastnorthasbng(easting: Column, northing: Column, resolution: String): Column =
        bng_eastnorthasbng(easting, northing, lit(resolution))
    def bng_geomkloop(geom: Column, resolution: Int, k: Int): Column = bng_geomkloop(geom, lit(resolution), lit(k))
    def bng_geomkloop(geom: Column, resolution: String, k: Int): Column = bng_geomkloop(geom, lit(resolution), lit(k))
    def bng_geomkloop(geom: Column, resolution: Int, k: Int, mode: String): Column = bng_geomkloop(geom, lit(resolution), lit(k), lit(mode))
    def bng_geomkloop(geom: Column, resolution: String, k: Int, mode: String): Column = bng_geomkloop(geom, lit(resolution), lit(k), lit(mode))
    def bng_geomkloop(geom: Column, resolution: Int, k: Int, mode: String, coverage: String): Column = bng_geomkloop(geom, lit(resolution), lit(k), lit(mode), lit(coverage))
    def bng_geomkloop(geom: Column, resolution: String, k: Int, mode: String, coverage: String): Column = bng_geomkloop(geom, lit(resolution), lit(k), lit(mode), lit(coverage))
    def bng_geomkring(geom: Column, resolution: Int, k: Int): Column = bng_geomkring(geom, lit(resolution), lit(k))
    def bng_geomkring(geom: Column, resolution: String, k: Int): Column = bng_geomkring(geom, lit(resolution), lit(k))
    def bng_geomkring(geom: Column, resolution: Int, k: Int, mode: String): Column = bng_geomkring(geom, lit(resolution), lit(k), lit(mode))
    def bng_geomkring(geom: Column, resolution: String, k: Int, mode: String): Column = bng_geomkring(geom, lit(resolution), lit(k), lit(mode))
    def bng_geomkring(geom: Column, resolution: Int, k: Int, mode: String, coverage: String): Column = bng_geomkring(geom, lit(resolution), lit(k), lit(mode), lit(coverage))
    def bng_geomkring(geom: Column, resolution: String, k: Int, mode: String, coverage: String): Column = bng_geomkring(geom, lit(resolution), lit(k), lit(mode), lit(coverage))
    def bng_kloop(cellid: Column, k: Int): Column = bng_kloop(cellid, lit(k))
    def bng_kring(cellid: Column, k: Int): Column = bng_kring(cellid, lit(k))
    def bng_pointascell(geom: Column, resolution: Int): Column = bng_pointascell(geom, lit(resolution))
    def bng_pointascell(geom: Column, resolution: String): Column = bng_pointascell(geom, lit(resolution))
    def bng_polyfill(geom: Column, resolution: Int): Column = bng_polyfill(geom, lit(resolution))
    def bng_polyfill(geom: Column, resolution: String): Column = bng_polyfill(geom, lit(resolution))
    def bng_tessellate(geom: Column, resolution: Int): Column = bng_tessellate(geom, lit(resolution))
    def bng_tessellate(geom: Column, resolution: String): Column = bng_tessellate(geom, lit(resolution))
    def bng_tessellate(geom: Column, resolution: Int, keepCoreGeom: Boolean): Column =
        bng_tessellate(geom, lit(resolution), lit(keepCoreGeom))
    def bng_tessellate(geom: Column, resolution: String, keepCoreGeom: Boolean): Column =
        bng_tessellate(geom, lit(resolution), lit(keepCoreGeom))
    def bng_geomkloopexplode(geom: Column, resolution: Int, k: Int): Column = bng_geomkloopexplode(geom, lit(resolution), lit(k))
    def bng_geomkloopexplode(geom: Column, resolution: String, k: Int): Column = bng_geomkloopexplode(geom, lit(resolution), lit(k))
    def bng_geomkloopexplode(geom: Column, resolution: Int, k: Int, mode: String): Column = bng_geomkloopexplode(geom, lit(resolution), lit(k), lit(mode))
    def bng_geomkloopexplode(geom: Column, resolution: String, k: Int, mode: String): Column = bng_geomkloopexplode(geom, lit(resolution), lit(k), lit(mode))
    def bng_geomkloopexplode(geom: Column, resolution: Int, k: Int, mode: String, coverage: String): Column = bng_geomkloopexplode(geom, lit(resolution), lit(k), lit(mode), lit(coverage))
    def bng_geomkloopexplode(geom: Column, resolution: String, k: Int, mode: String, coverage: String): Column = bng_geomkloopexplode(geom, lit(resolution), lit(k), lit(mode), lit(coverage))
    def bng_geomkringexplode(geom: Column, resolution: Int, k: Int): Column = bng_geomkringexplode(geom, lit(resolution), lit(k))
    def bng_geomkringexplode(geom: Column, resolution: String, k: Int): Column = bng_geomkringexplode(geom, lit(resolution), lit(k))
    def bng_geomkringexplode(geom: Column, resolution: Int, k: Int, mode: String): Column = bng_geomkringexplode(geom, lit(resolution), lit(k), lit(mode))
    def bng_geomkringexplode(geom: Column, resolution: String, k: Int, mode: String): Column = bng_geomkringexplode(geom, lit(resolution), lit(k), lit(mode))
    def bng_geomkringexplode(geom: Column, resolution: Int, k: Int, mode: String, coverage: String): Column = bng_geomkringexplode(geom, lit(resolution), lit(k), lit(mode), lit(coverage))
    def bng_geomkringexplode(geom: Column, resolution: String, k: Int, mode: String, coverage: String): Column = bng_geomkringexplode(geom, lit(resolution), lit(k), lit(mode), lit(coverage))
    def bng_kloopexplode(cellid: Column, k: Int): Column = bng_kloopexplode(cellid, lit(k))
    def bng_kringexplode(cellid: Column, k: Int): Column = bng_kringexplode(cellid, lit(k))
    def bng_tessellateexplode(geom: Column, resolution: Int): Column = bng_tessellateexplode(geom, lit(resolution))
    def bng_tessellateexplode(geom: Column, resolution: String): Column = bng_tessellateexplode(geom, lit(resolution))
    def bng_tessellateexplode(geom: Column, resolution: Int, keepCoreGeom: Boolean): Column =
        bng_tessellateexplode(geom, lit(resolution), lit(keepCoreGeom))
    def bng_tessellateexplode(geom: Column, resolution: String, keepCoreGeom: Boolean): Column =
        bng_tessellateexplode(geom, lit(resolution), lit(keepCoreGeom))

}
