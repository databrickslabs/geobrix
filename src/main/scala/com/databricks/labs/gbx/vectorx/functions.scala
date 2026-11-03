package com.databricks.labs.gbx.vectorx

import com.databricks.labs.gbx.vectorx.expressions.{ST_AsMvt, ST_Crs, ST_InterpolateElevationBBox, ST_InterpolateElevationGeom, ST_SetCrs, ST_TransformCrs, ST_Triangulate}
import com.databricks.labs.gbx.vectorx.jts.legacy.expressions.ST_LegacyAsWKB
import com.databricks.labs.gbx.vectorx.mvt.MvtWriter
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column

/**
  * VectorX API entry point: Column-based helpers for the expression-level `gbx_st_*` functions.
  *
  * Registration belongs to the Python manager (`databricks.labs.gbx.register(spark)`); this object registers nothing.
  * (VectorX data sources are registered separately via
  * `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`.)
  *
  * This package exposes the `gbx_st_asmvt` MVT aggregator (see [[ST_AsMvt]]), the `gbx_st_triangulate`
  * constrained-Delaunay TIN generator (see [[ST_Triangulate]]), and the
  * `gbx_st_interpolateelevationbbox`/`gbx_st_interpolateelevationgeom` elevation
  * generators (see [[ST_InterpolateElevationBBox]], [[ST_InterpolateElevationGeom]]).
  */
object functions extends Serializable {

    /**
      * Aggregator: encode a group of features into a Mapbox Vector Tile (MVT) protobuf blob.
      *
      * @param geom      per-row geometry in WKB (BINARY) in tile-local coordinates
      * @param attrs     per-row attribute struct (all fields stringified in v0.4.0)
      * @param layerName constant Column holding the MVT layer name
      */
    def st_asmvt(geom: Column, attrs: Column, layerName: Column): Column =
        ColumnAdapter(ST_AsMvt.name, Seq(geom, attrs, layerName))

    /** Convenience overload - pass a plain string as the layer name. */
    def st_asmvt(geom: Column, attrs: Column, layerName: String): Column =
        st_asmvt(geom, attrs, lit(layerName))

    /** TIN generator: one constrained Delaunay triangle (`triangle`, 2D WKB) per row. `mode` defaults to `constrained`. */
    def st_triangulate(
        pointsArray: Column, breaklinesArray: Column, mergeTolerance: Column, snapTolerance: Column,
        splitPointFinder: Column
    ): Column =
        ColumnAdapter(ST_Triangulate.name, Seq(pointsArray, breaklinesArray, mergeTolerance, snapTolerance, splitPointFinder))

    def st_triangulate(
        pointsArray: Column, breaklinesArray: Column, mergeTolerance: Column, snapTolerance: Column,
        splitPointFinder: Column, mode: Column
    ): Column =
        ColumnAdapter(ST_Triangulate.name, Seq(pointsArray, breaklinesArray, mergeTolerance, snapTolerance, splitPointFinder, mode))

    /** TIN generator: the interpolated Z (`elevation_point`, WKB point with Z) at each cell centre of a bounding-box grid. */
    def st_interpolateelevationbbox(
        pointsArray: Column, breaklinesArray: Column, mergeTolerance: Column, snapTolerance: Column,
        splitPointFinder: Column, xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        widthPx: Column, heightPx: Column, srid: Column
    ): Column =
        ColumnAdapter(
            ST_InterpolateElevationBBox.name,
            Seq(pointsArray, breaklinesArray, mergeTolerance, snapTolerance, splitPointFinder, xmin, ymin, xmax, ymax, widthPx, heightPx, srid)
        )

    def st_interpolateelevationbbox(
        pointsArray: Column, breaklinesArray: Column, mergeTolerance: Column, snapTolerance: Column,
        splitPointFinder: Column, xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        widthPx: Column, heightPx: Column, srid: Column, mode: Column
    ): Column =
        ColumnAdapter(
            ST_InterpolateElevationBBox.name,
            Seq(pointsArray, breaklinesArray, mergeTolerance, snapTolerance, splitPointFinder, xmin, ymin, xmax, ymax, widthPx, heightPx, srid, mode)
        )

    /** TIN generator: the interpolated Z at each cell centre of a grid anchored at a point. */
    def st_interpolateelevationgeom(
        pointsArray: Column, breaklinesArray: Column, mergeTolerance: Column, snapTolerance: Column,
        splitPointFinder: Column, gridOrigin: Column, gridCols: Column, gridRows: Column,
        cellSizeX: Column, cellSizeY: Column
    ): Column =
        ColumnAdapter(
            ST_InterpolateElevationGeom.name,
            Seq(pointsArray, breaklinesArray, mergeTolerance, snapTolerance, splitPointFinder, gridOrigin, gridCols, gridRows, cellSizeX, cellSizeY)
        )

    def st_interpolateelevationgeom(
        pointsArray: Column, breaklinesArray: Column, mergeTolerance: Column, snapTolerance: Column,
        splitPointFinder: Column, gridOrigin: Column, gridCols: Column, gridRows: Column,
        cellSizeX: Column, cellSizeY: Column, mode: Column
    ): Column =
        ColumnAdapter(
            ST_InterpolateElevationGeom.name,
            Seq(pointsArray, breaklinesArray, mergeTolerance, snapTolerance, splitPointFinder, gridOrigin, gridCols, gridRows, cellSizeX, cellSizeY, mode)
        )

    /** Decodes a legacy Mosaic geometry struct (`typeId`, `srid`, `boundaries`, `holes`) to ISO WKB, Z and holes kept. */
    def st_legacyaswkb(geom: Column): Column = ColumnAdapter(ST_LegacyAsWKB.name, Seq(geom))

    /**
      * Returns the canonical CRS string (e.g. `"EPSG:4326"`, `"ESRI:54008"`) for a geometry's
      * embedded SRID, or null for plain WKB/WKT (no embedded SRID), null inputs, or unresolvable
      * SRIDs (never-error: an unrecognized code returns null, not an exception).
      */
    def st_crs(geom: Column): Column = ColumnAdapter(ST_Crs.name, Seq(geom))

    /**
      * Stamps a CRS on a geometry without reprojecting. Returns BINARY (EWKB) for both
      * BINARY and STRING geometry inputs — the SQL surface has one fixed return type.
      * A CRS with no integer authority code (raw WKT / PROJ4, or a non-numeric code such
      * as `OGC:CRS84`) raises: a geometry SRID must be an integer.
      */
    def st_setcrs(geom: Column, crs: Column): Column =
        ColumnAdapter(ST_SetCrs.name, Seq(geom, crs))

    /** Convenience overload — string CRS literal. */
    def st_setcrs(geom: Column, crs: String): Column = st_setcrs(geom, lit(crs))

    /**
      * Reproject geometry to ``targetCrs``. Optional ``sourceCrs`` for plain (SRID-less) inputs.
      * Returns input unchanged if source CRS is unresolvable (never-error invariant).
      * Returns BINARY for both BINARY and STRING geometry inputs: EWKB with the target SRID
      * when the target carries an integer authority code, plain WKB when it does not.
      */
    def st_transformcrs(geom: Column, targetCrs: Column): Column =
        ColumnAdapter(ST_TransformCrs.name, Seq(geom, targetCrs))

    /** Convenience overload — string target CRS literal. */
    def st_transformcrs(geom: Column, targetCrs: String): Column =
        st_transformcrs(geom, lit(targetCrs))

    /** 3-argument form: explicit source CRS for plain (SRID-less) inputs. */
    def st_transformcrs(geom: Column, targetCrs: Column, sourceCrs: Column): Column =
        ColumnAdapter(ST_TransformCrs.name, Seq(geom, targetCrs, sourceCrs))

    /** Convenience overload — string target and source CRS literals. */
    def st_transformcrs(geom: Column, targetCrs: String, sourceCrs: String): Column =
        st_transformcrs(geom, lit(targetCrs), lit(sourceCrs))

}
