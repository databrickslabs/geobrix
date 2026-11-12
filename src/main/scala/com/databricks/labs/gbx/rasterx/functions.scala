package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.rasterx.expressions.accessors._
import com.databricks.labs.gbx.rasterx.expressions.agg.{RST_BNG_RasterizeAgg, RST_Custom_RasterizeAgg, RST_H3_RasterizeAgg, RST_Quadbin_RasterizeAgg}
import com.databricks.labs.gbx.rasterx.expressions.constructor.RST_FromContent
import com.databricks.labs.gbx.rasterx.expressions.web._
import com.databricks.labs.gbx.rasterx.expressions._
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column

/**
  * RasterX API entry point: Column-based helpers (e.g. `rst_width`) for the `gbx_rst_*` functions.
  *
  * The helpers call the functions by SQL name, so they need the session to have them. Registration belongs to
  * the Python manager (`databricks.labs.gbx.register(spark)`); this object registers nothing.
  */
object functions extends Serializable {

    // Accessors
    def rst_boundingbox(tile: Column): Column = ColumnAdapter(RST_BoundingBox.name, Seq(tile))
    def rst_numbands(tile: Column): Column = ColumnAdapter(RST_NumBands.name, Seq(tile))
    def rst_upperlefty(tile: Column): Column = ColumnAdapter(RST_UpperLeftY.name, Seq(tile))
    def rst_width(tile: Column): Column = ColumnAdapter(RST_Width.name, Seq(tile))

    // Aggregators
    /** UDAF: rasterize a group's H3 cells into one tile (pixel-centroid burn).
     *  Auto-derives the grid from the cell set; value omitted -> presence mask (1.0, an untyped NULL value). */
    def rst_h3_rasterize_agg(cellid: Column): Column =
        ColumnAdapter(RST_H3_RasterizeAgg.name, Seq(
            cellid, lit(null), lit(4326), lit(null).cast("double"),
            lit(null).cast("double"), lit(null).cast("double"),
            lit(null).cast("double"), lit(null).cast("double"),
            lit(null).cast("int"), lit(null).cast("int"),
            lit("centroids"), lit(1)
        ))
    def rst_h3_rasterize_agg(cellid: Column, value: Column): Column =
        ColumnAdapter(RST_H3_RasterizeAgg.name, Seq(
            cellid, value, lit(4326), lit(null).cast("double"),
            lit(null).cast("double"), lit(null).cast("double"),
            lit(null).cast("double"), lit(null).cast("double"),
            lit(null).cast("int"), lit(null).cast("int"),
            lit("centroids"), lit(1)
        ))
    def rst_h3_rasterize_agg(
        cellid: Column, value: Column, out_srid: Column, pixelSize: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        width: Column, height: Column, mode: Column, kringPad: Column
    ): Column =
        ColumnAdapter(RST_H3_RasterizeAgg.name, Seq(
            cellid, value, out_srid, pixelSize, xmin, ymin, xmax, ymax, width, height, mode, kringPad
        ))

    /** As above, with the trailing `out_crs` (`EPSG:<n>`; it wins over `out_srid`). */
    def rst_h3_rasterize_agg(
        cellid: Column, value: Column, out_srid: Column, pixelSize: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        width: Column, height: Column, mode: Column, kringPad: Column, outCrs: Column
    ): Column =
        ColumnAdapter(RST_H3_RasterizeAgg.name, Seq(
            cellid, value, out_srid, pixelSize, xmin, ymin, xmax, ymax, width, height, mode, kringPad, outCrs
        ))

    /** UDAF: rasterize a group's quadbin cells into one tile (pixel-centroid burn).
     *  Auto-derives the grid from the cell set; value omitted -> presence mask (1.0, an untyped NULL value). */
    def rst_quadbin_rasterize_agg(cellid: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterizeAgg.name, Seq(
            cellid, lit(null), lit(4326), lit(null).cast("double"),
            lit(null).cast("double"), lit(null).cast("double"),
            lit(null).cast("double"), lit(null).cast("double"),
            lit(null).cast("int"), lit(null).cast("int"),
            lit("centroids"), lit(1)
        ))
    def rst_quadbin_rasterize_agg(cellid: Column, value: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterizeAgg.name, Seq(
            cellid, value, lit(4326), lit(null).cast("double"),
            lit(null).cast("double"), lit(null).cast("double"),
            lit(null).cast("double"), lit(null).cast("double"),
            lit(null).cast("int"), lit(null).cast("int"),
            lit("centroids"), lit(1)
        ))
    def rst_quadbin_rasterize_agg(
        cellid: Column, value: Column, out_srid: Column, pixelSize: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        width: Column, height: Column, mode: Column, kringPad: Column
    ): Column =
        ColumnAdapter(RST_Quadbin_RasterizeAgg.name, Seq(
            cellid, value, out_srid, pixelSize, xmin, ymin, xmax, ymax, width, height, mode, kringPad
        ))

    /** As above, with the trailing `out_crs` (`EPSG:<n>`; it wins over `out_srid`). */
    def rst_quadbin_rasterize_agg(
        cellid: Column, value: Column, out_srid: Column, pixelSize: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        width: Column, height: Column, mode: Column, kringPad: Column, outCrs: Column
    ): Column =
        ColumnAdapter(RST_Quadbin_RasterizeAgg.name, Seq(
            cellid, value, out_srid, pixelSize, xmin, ymin, xmax, ymax, width, height, mode, kringPad, outCrs
        ))

    /** UDAF: rasterize a group's BNG cells (STRING ids) into one 27700-native tile.
     *  The `srid` argument is retained for signature parity but forced to 27700 (no-op);
     *  auto-derives the grid from the cell set; value omitted -> presence mask (1.0, an untyped NULL value). */
    def rst_bng_rasterize_agg(cellid: Column): Column =
        ColumnAdapter(RST_BNG_RasterizeAgg.name, Seq(
            cellid, lit(null), lit(27700), lit(null).cast("double"),
            lit(null).cast("double"), lit(null).cast("double"),
            lit(null).cast("double"), lit(null).cast("double"),
            lit(null).cast("int"), lit(null).cast("int"),
            lit("centroids"), lit(1)
        ))
    def rst_bng_rasterize_agg(cellid: Column, value: Column): Column =
        ColumnAdapter(RST_BNG_RasterizeAgg.name, Seq(
            cellid, value, lit(27700), lit(null).cast("double"),
            lit(null).cast("double"), lit(null).cast("double"),
            lit(null).cast("double"), lit(null).cast("double"),
            lit(null).cast("int"), lit(null).cast("int"),
            lit("centroids"), lit(1)
        ))
    def rst_bng_rasterize_agg(
        cellid: Column, value: Column, out_srid: Column, pixelSize: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        width: Column, height: Column, mode: Column, kringPad: Column
    ): Column =
        ColumnAdapter(RST_BNG_RasterizeAgg.name, Seq(
            cellid, value, out_srid, pixelSize, xmin, ymin, xmax, ymax, width, height, mode, kringPad
        ))

    /** As above, with the trailing `out_crs`: a non-NULL value is refused on both tiers (BNG output is always
      * EPSG:27700; pass NULL and reproject with `rst_transformcrs`). */
    def rst_bng_rasterize_agg(
        cellid: Column, value: Column, out_srid: Column, pixelSize: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        width: Column, height: Column, mode: Column, kringPad: Column, outCrs: Column
    ): Column =
        ColumnAdapter(RST_BNG_RasterizeAgg.name, Seq(
            cellid, value, out_srid, pixelSize, xmin, ymin, xmax, ymax, width, height, mode, kringPad, outCrs
        ))

    /** UDAF: rasterize a group's custom-grid cells into one tile (pixel-centroid burn).
     *  Requires a `grid` struct column (produced by `gbx_custom_grid(...)`).
     *  Auto-derives the extent from the cell set; value omitted -> presence mask (1.0, an untyped NULL value). */
    def rst_custom_rasterize_agg(cellid: Column, value: Column, grid: Column): Column =
        ColumnAdapter(RST_Custom_RasterizeAgg.name, Seq(
            cellid, value, grid,
            lit(null).cast("int"), lit(null).cast("double"),
            lit(null).cast("double"), lit(null).cast("double"),
            lit(null).cast("double"), lit(null).cast("double"),
            lit(null).cast("int"), lit(null).cast("int"),
            lit("centroids"), lit(1)
        ))
    def rst_custom_rasterize_agg(
        cellid: Column, value: Column, grid: Column,
        outSrid: Column, pixelSize: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        width: Column, height: Column, mode: Column, kringPad: Column
    ): Column =
        ColumnAdapter(RST_Custom_RasterizeAgg.name, Seq(
            cellid, value, grid, outSrid, pixelSize,
            xmin, ymin, xmax, ymax, width, height, mode, kringPad
        ))

    // Constructors
    def rst_fromcontent(content: Column, driver: Column): Column = ColumnAdapter(RST_FromContent.name, Seq(content, driver))
    // rst_fromfile is lightweight-only (Python UDF); no Scala/JVM column helper (see register/#34).

    // Operations
    def rst_rastertoworldcoordx(tile: Column, x: Column, y: Column): Column =
        ColumnAdapter(RST_RasterToWorldCoordX.name, Seq(tile, x, y))
    def rst_worldtorastercoordx(tile: Column, x: Column, y: Column): Column =
        ColumnAdapter(RST_WorldToRasterCoordX.name, Seq(tile, x, y))
    def rst_worldtorastercoordy(tile: Column, x: Column, y: Column): Column =
        ColumnAdapter(RST_WorldToRasterCoordY.name, Seq(tile, x, y))

    // Scalar-literal overloads — so users can pass plain values for non-Column params
    // (e.g. rst_clip(tile, clip, true) instead of rst_clip(tile, clip, lit(true))).
    // Column params (tile, geometry, kernel, tiles, content) stay as Column.
    def rst_fromcontent(content: Column, driver: String): Column = rst_fromcontent(content, lit(driver))
    // rst_fromfile is lightweight-only (Python UDF); no Scala/JVM column helper or scalar
    // overloads (the JVM cannot read UC Volumes -- see register/#34). Use the Python/SQL binding.
    def rst_rastertoworldcoordx(tile: Column, x: Int, y: Int): Column =
        rst_rastertoworldcoordx(tile, lit(x), lit(y))
    def rst_worldtorastercoordx(tile: Column, x: Double, y: Double): Column =
        rst_worldtorastercoordx(tile, lit(x), lit(y))
    def rst_worldtorastercoordy(tile: Column, x: Double, y: Double): Column =
        rst_worldtorastercoordy(tile, lit(x), lit(y))

    def rst_tilexyz(tile: Column, z: Column, x: Column, y: Column): Column =
        ColumnAdapter(RST_TileXYZ.name, Seq(tile, z, x, y, lit("PNG"), lit(256), lit("bilinear")))
    def rst_tilexyz(
        tile: Column, z: Column, x: Column, y: Column,
        format: Column, size: Column, resampling: Column
    ): Column =
        ColumnAdapter(RST_TileXYZ.name, Seq(tile, z, x, y, format, size, resampling))
    def rst_tilexyz(tile: Column, z: Int, x: Int, y: Int): Column =
        rst_tilexyz(tile, lit(z), lit(x), lit(y))
    def rst_tilexyz(
        tile: Column, z: Int, x: Int, y: Int,
        format: String, size: Int, resampling: String
    ): Column =
        rst_tilexyz(tile, lit(z), lit(x), lit(y), lit(format), lit(size), lit(resampling))

}
