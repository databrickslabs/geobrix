package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.expressions.RegistryDelegate
import com.databricks.labs.gbx.rasterx.expressions.accessors._
import com.databricks.labs.gbx.rasterx.expressions.agg.{RST_BNG_RasterizeAgg, RST_CombineAvgAgg, RST_DerivedBandAgg, RST_FromBandsAgg, RST_H3_RasterizeAgg, RST_MergeAgg, RST_Quadbin_RasterizeAgg, RST_RasterizeAgg}
import com.databricks.labs.gbx.rasterx.expressions.analysis._
import com.databricks.labs.gbx.rasterx.expressions.constructor.{RST_FromBands, RST_FromContent}
import com.databricks.labs.gbx.rasterx.expressions.dem._
import com.databricks.labs.gbx.rasterx.expressions.generators._
import com.databricks.labs.gbx.rasterx.expressions.grid._
import com.databricks.labs.gbx.rasterx.expressions.pixel._
import com.databricks.labs.gbx.rasterx.expressions.resample._
import com.databricks.labs.gbx.rasterx.expressions.spectral._
import com.databricks.labs.gbx.rasterx.expressions.vector.{RST_Polygonize, RST_Rasterize}
import com.databricks.labs.gbx.rasterx.expressions.web._
import com.databricks.labs.gbx.rasterx.expressions._
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

/**
  * RasterX API entry point: register all raster SQL functions and provide Column-based helpers.
  *
  * Call `functions.register(spark)` once per session to make `gbx_rst_*` functions available in SQL
  * and to initialize GDAL. The Column helpers (e.g. `rst_width`) delegate to
  * the same registered functions.
  */
object functions extends Serializable {

    val flag = "com.databricks.labs.gbx.rasterx.registered"

    /** Register all RasterX expressions with Spark and initialize GDAL; idempotent per session. */
    def register(spark: SparkSession): Unit = {
        val sc = spark.sparkContext
        if (sc.getConf.get(flag, "false") == "true") return

        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)

        // Accessors
        rd.register(RST_Avg)
        rd.register(RST_BandMetaData)
        rd.register(RST_BoundingBox)
        rd.register(RST_Format)
        rd.register(RST_GeoReference)
        rd.register(RST_GetNoData)
        rd.register(RST_GetSubdataset)
        rd.register(RST_Height)
        rd.register(RST_Max)
        rd.register(RST_Median)
        rd.register(RST_MemSize)
        rd.register(RST_MetaData)
        rd.register(RST_Min)
        rd.register(RST_NumBands)
        rd.register(RST_PixelCount)
        rd.register(RST_PixelHeight)
        rd.register(RST_PixelWidth)
        rd.register(RST_Rotation)
        rd.register(RST_ScaleX)
        rd.register(RST_ScaleY)
        rd.register(RST_SkewX)
        rd.register(RST_SkewY)
        rd.register(RST_Crs)
        rd.register(RST_SRID)
        rd.register(RST_Subdatasets)
        rd.register(RST_Summary)
        rd.register(RST_Type)
        rd.register(RST_UpperLeftX)
        rd.register(RST_UpperLeftY)
        rd.register(RST_Width)

        // Aggregators
        rd.register(RST_CombineAvgAgg)
        rd.register(RST_DerivedBandAgg)
        rd.register(RST_DTMFromGeomsAgg)
        rd.register(RST_FromBandsAgg)
        rd.register(RST_MergeAgg)
        rd.register(RST_RasterizeAgg)
        rd.register(RST_H3_RasterizeAgg)
        rd.register(RST_Quadbin_RasterizeAgg)
        rd.register(RST_BNG_RasterizeAgg)

        // Constructors
        rd.register(RST_FromBands)
        rd.register(RST_FromContent)
        // gbx_rst_fromfile is NOT registered here: it cannot be implemented in the JVM tier
        // (the executor JVM lacks the UC FUSE credential for /Volumes). It is registered as a
        // Python UDF in the lightweight tier (databricks.labs.gbx.rasterx.functions.register ->
        // pyrx), accessible from Python and SQL when a light-tier extra is installed. Issue #34.

        // Generators
        rd.register(RST_H3_Tessellate)
        rd.register(RST_Quadbin_Tessellate)
        rd.register(RST_BNG_Tessellate)
        rd.register(RST_MakeTiles)
        rd.register(RST_ReTile)
        rd.register(RST_SeparateBands)
        rd.register(RST_ToOverlappingTiles)

        // Grid
        rd.register(RST_H3_RasterToGridAvg)
        rd.register(RST_H3_RasterToGridCount)
        rd.register(RST_H3_RasterToGridMax)
        rd.register(RST_H3_RasterToGridMin)
        rd.register(RST_H3_RasterToGridMedian)
        rd.register(RST_H3_RasterToGridSum)
        rd.register(RST_H3_RasterToGridVariance)
        rd.register(RST_H3_RasterToGridStddev)
        rd.register(RST_Quadbin_RasterToGridAvg)
        rd.register(RST_Quadbin_RasterToGridCount)
        rd.register(RST_Quadbin_RasterToGridMax)
        rd.register(RST_Quadbin_RasterToGridMin)
        rd.register(RST_Quadbin_RasterToGridMedian)
        rd.register(RST_Quadbin_RasterToGridSum)
        rd.register(RST_Quadbin_RasterToGridVariance)
        rd.register(RST_Quadbin_RasterToGridStddev)
        rd.register(RST_BNG_RasterToGridAvg)
        rd.register(RST_BNG_RasterToGridCount)
        rd.register(RST_BNG_RasterToGridMax)
        rd.register(RST_BNG_RasterToGridMin)
        rd.register(RST_BNG_RasterToGridMedian)
        rd.register(RST_BNG_RasterToGridSum)
        rd.register(RST_BNG_RasterToGridVariance)
        rd.register(RST_BNG_RasterToGridStddev)
        rd.register(RST_H3_CellBBox)

        // Operations
        rd.register(RST_AsFormat)
        rd.register(RST_Clip)
        rd.register(RST_CombineAvg)
        rd.register(RST_Convolve)
        rd.register(RST_DerivedBand)
        rd.register(RST_DTMFromGeoms)
        rd.register(RST_Filter)
        rd.register(RST_InitNoData)
        rd.register(RST_IsEmpty)
        rd.register(RST_MapAlgebra)
        rd.register(RST_Merge)
        rd.register(RST_NDVI)
        rd.register(RST_RasterToWorldCoord)
        rd.register(RST_RasterToWorldCoordX)
        rd.register(RST_RasterToWorldCoordY)
        rd.register(RST_Transform)
        rd.register(RST_TransformCrs)
        rd.register(RST_TryOpen)
        rd.register(RST_UpdateType)
        rd.register(RST_WorldToRasterCoord)
        rd.register(RST_WorldToRasterCoordX)
        rd.register(RST_WorldToRasterCoordY)

        // Web-mercator tile output
        rd.register(RST_ToWebMercator)
        rd.register(RST_TileXYZ)
        rd.register(RST_XYZPyramid)

        // Vector<->raster bridge
        rd.register(RST_Rasterize)
        rd.register(RST_Polygonize)

        // Terrain analysis (DEM processing)
        rd.register(RST_Aspect)
        rd.register(RST_ColorRelief)
        rd.register(RST_Hillshade)
        rd.register(RST_Roughness)
        rd.register(RST_Slope)
        rd.register(RST_TPI)
        rd.register(RST_TRI)

        // Spectral indices (multi-band satellite math over RST_MapAlgebra)
        rd.register(RST_EVI)
        rd.register(RST_Index)
        rd.register(RST_NBR)
        rd.register(RST_NDWI)
        rd.register(RST_SAVI)

        // Resample (gdal.Warp -tr/-ts wrappers) + IDW (gdal.Grid invdist)
        rd.register(RST_Resample)
        rd.register(RST_ResampleToSize)
        rd.register(RST_ResampleToRes)
        rd.register(RST_GridFromPoints)
        rd.register(RST_GridFromPointsAgg)

        // Pixel ops + extraction (thin GDAL wrappers)
        rd.register(RST_Band)
        rd.register(RST_BuildOverviews)
        rd.register(RST_FillNodata)
        rd.register(RST_Histogram)
        rd.register(RST_Sample)
        rd.register(RST_SetSrid)
        rd.register(RST_SetCrs)
        rd.register(RST_Threshold)

        // Analysis (COG / proximity / contour / viewshed — GDAL wrappers)
        rd.register(RST_CogConvert)
        rd.register(RST_Contour)
        rd.register(RST_Proximity)
        rd.register(RST_Viewshed)

        sc.getConf.set(flag, "true")
    }

    // Accessors
    def rst_avg(tile: Column): Column = ColumnAdapter(RST_Avg.name, Seq(tile))
    def rst_bandmetadata(tile: Column, band: Column): Column = ColumnAdapter(RST_BandMetaData.name, Seq(tile, band))
    def rst_boundingbox(tile: Column): Column = ColumnAdapter(RST_BoundingBox.name, Seq(tile))
    def rst_format(tile: Column): Column = ColumnAdapter(RST_Format.name, Seq(tile))
    def rst_georeference(tile: Column): Column = ColumnAdapter(RST_GeoReference.name, Seq(tile))
    def rst_getnodata(tile: Column): Column = ColumnAdapter(RST_GetNoData.name, Seq(tile))
    def rst_getsubdataset(tile: Column, subsetName: Column): Column = ColumnAdapter(RST_GetSubdataset.name, Seq(tile, subsetName))
    def rst_height(tile: Column): Column = ColumnAdapter(RST_Height.name, Seq(tile))
    def rst_max(tile: Column): Column = ColumnAdapter(RST_Max.name, Seq(tile))
    def rst_median(tile: Column): Column = ColumnAdapter(RST_Median.name, Seq(tile))
    def rst_memsize(tile: Column): Column = ColumnAdapter(RST_MemSize.name, Seq(tile))
    def rst_metadata(tile: Column): Column = ColumnAdapter(RST_MetaData.name, Seq(tile))
    def rst_min(tile: Column): Column = ColumnAdapter(RST_Min.name, Seq(tile))
    def rst_numbands(tile: Column): Column = ColumnAdapter(RST_NumBands.name, Seq(tile))
    def rst_pixelcount(tile: Column): Column = ColumnAdapter(RST_PixelCount.name, Seq(tile))
    def rst_pixelheight(tile: Column): Column = ColumnAdapter(RST_PixelHeight.name, Seq(tile))
    def rst_pixelwidth(tile: Column): Column = ColumnAdapter(RST_PixelWidth.name, Seq(tile))
    def rst_rotation(tile: Column): Column = ColumnAdapter(RST_Rotation.name, Seq(tile))
    def rst_scalex(tile: Column): Column = ColumnAdapter(RST_ScaleX.name, Seq(tile))
    def rst_scaley(tile: Column): Column = ColumnAdapter(RST_ScaleY.name, Seq(tile))
    def rst_skewx(tile: Column): Column = ColumnAdapter(RST_SkewX.name, Seq(tile))
    def rst_skewy(tile: Column): Column = ColumnAdapter(RST_SkewY.name, Seq(tile))
    def rst_crs(tile: Column): Column = ColumnAdapter(RST_Crs.name, Seq(tile))
    def rst_srid(tile: Column): Column = ColumnAdapter(RST_SRID.name, Seq(tile))
    def rst_subdatasets(tile: Column): Column = ColumnAdapter(RST_Subdatasets.name, Seq(tile))
    def rst_summary(tile: Column): Column = ColumnAdapter(RST_Summary.name, Seq(tile))
    def rst_type(tile: Column): Column = ColumnAdapter(RST_Type.name, Seq(tile))
    def rst_upperleftx(tile: Column): Column = ColumnAdapter(RST_UpperLeftX.name, Seq(tile))
    def rst_upperlefty(tile: Column): Column = ColumnAdapter(RST_UpperLeftY.name, Seq(tile))
    def rst_width(tile: Column): Column = ColumnAdapter(RST_Width.name, Seq(tile))

    // Aggregators
def rst_combineavg_agg(tile: Column): Column = ColumnAdapter(RST_CombineAvgAgg.name, Seq(tile))
    def rst_derivedband_agg(tile: Column, pyfunc: String, funcName: String): Column =
      ColumnAdapter(RST_DerivedBandAgg.name, Seq(tile, lit(pyfunc), lit(funcName)))
    def rst_merge_agg(tile: Column): Column = ColumnAdapter(RST_MergeAgg.name, Seq(tile))

    /** UDAF: rasterize a group's H3 cells into one tile (pixel-centroid burn).
     *  Auto-derives the grid from the cell set; value omitted -> presence mask (1.0). */
    def rst_h3_rasterize_agg(cellid: Column): Column =
        ColumnAdapter(RST_H3_RasterizeAgg.name, Seq(
            cellid, lit(null).cast("double"), lit(4326), lit(null).cast("double"),
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

    /** UDAF: rasterize a group's quadbin cells into one tile (pixel-centroid burn).
     *  Auto-derives the grid from the cell set; value omitted -> presence mask (1.0). */
    def rst_quadbin_rasterize_agg(cellid: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterizeAgg.name, Seq(
            cellid, lit(null).cast("double"), lit(4326), lit(null).cast("double"),
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

    /** UDAF: rasterize a group's BNG cells (STRING ids) into one 27700-native tile.
     *  The `srid` argument is retained for signature parity but forced to 27700 (no-op);
     *  auto-derives the grid from the cell set; value omitted -> presence mask (1.0). */
    def rst_bng_rasterize_agg(cellid: Column): Column =
        ColumnAdapter(RST_BNG_RasterizeAgg.name, Seq(
            cellid, lit(null).cast("double"), lit(27700), lit(null).cast("double"),
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

    // Constructors
    def rst_fromcontent(content: Column, driver: Column): Column = ColumnAdapter(RST_FromContent.name, Seq(content, driver))
    // rst_fromfile is lightweight-only (Python UDF); no Scala/JVM column helper (see register/#34).
    def rst_frombands(bands: Column): Column = ColumnAdapter(RST_FromBands.name, Seq(bands))

    // Generators
    def rst_h3_tessellate(tile: Column, resolution: Column): Column = ColumnAdapter(RST_H3_Tessellate.name, Seq(tile, resolution))
    def rst_h3_tessellate(tile: Column, resolution: Column, mode: String): Column =
        ColumnAdapter(RST_H3_Tessellate.name, Seq(tile, resolution, lit(mode)))
    def rst_quadbin_tessellate(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_Tessellate.name, Seq(tile, resolution))
    def rst_quadbin_tessellate(tile: Column, resolution: Column, mode: String): Column =
        ColumnAdapter(RST_Quadbin_Tessellate.name, Seq(tile, resolution, lit(mode)))
    def rst_bng_tessellate(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_Tessellate.name, Seq(tile, resolution))
    def rst_bng_tessellate(tile: Column, resolution: Column, mode: String): Column =
        ColumnAdapter(RST_BNG_Tessellate.name, Seq(tile, resolution, lit(mode)))
    def rst_maketiles(tile: Column, sizeInMB: Column): Column =
        ColumnAdapter(RST_MakeTiles.name, Seq(tile, sizeInMB))
    def rst_retile(tile: Column, tileWidth: Column, tileHeight: Column): Column =
        ColumnAdapter(RST_ReTile.name, Seq(tile, tileWidth, tileHeight))
    def rst_separatebands(tile: Column): Column = ColumnAdapter(RST_SeparateBands.name, Seq(tile))
    def rst_tooverlappingtiles(tile: Column, tileWidth: Column, tileHeight: Column, overlap: Column): Column =
        ColumnAdapter(RST_ToOverlappingTiles.name, Seq(tile, tileWidth, tileHeight, overlap))

    // Grid
    def rst_h3_rastertogridavg(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridAvg.name, Seq(tile, resolution))
    def rst_h3_rastertogridcount(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridCount.name, Seq(tile, resolution))
    def rst_h3_rastertogridmax(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridMax.name, Seq(tile, resolution))
    def rst_h3_rastertogridmin(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridMin.name, Seq(tile, resolution))
    def rst_h3_rastertogridmedian(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridMedian.name, Seq(tile, resolution))
    def rst_h3_rastertogridsum(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridSum.name, Seq(tile, resolution))
    def rst_h3_rastertogridvariance(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridVariance.name, Seq(tile, resolution))
    def rst_h3_rastertogridstddev(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridStddev.name, Seq(tile, resolution))
    def rst_quadbin_rastertogridavg(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridAvg.name, Seq(tile, resolution))
    def rst_quadbin_rastertogridcount(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridCount.name, Seq(tile, resolution))
    def rst_quadbin_rastertogridmax(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridMax.name, Seq(tile, resolution))
    def rst_quadbin_rastertogridmin(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridMin.name, Seq(tile, resolution))
    def rst_quadbin_rastertogridmedian(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridMedian.name, Seq(tile, resolution))
    def rst_quadbin_rastertogridsum(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridSum.name, Seq(tile, resolution))
    def rst_quadbin_rastertogridvariance(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridVariance.name, Seq(tile, resolution))
    def rst_quadbin_rastertogridstddev(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridStddev.name, Seq(tile, resolution))
    def rst_bng_rastertogridavg(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridAvg.name, Seq(tile, resolution))
    def rst_bng_rastertogridcount(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridCount.name, Seq(tile, resolution))
    def rst_bng_rastertogridmax(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridMax.name, Seq(tile, resolution))
    def rst_bng_rastertogridmin(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridMin.name, Seq(tile, resolution))
    def rst_bng_rastertogridmedian(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridMedian.name, Seq(tile, resolution))
    def rst_bng_rastertogridsum(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridSum.name, Seq(tile, resolution))
    def rst_bng_rastertogridvariance(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridVariance.name, Seq(tile, resolution))
    def rst_bng_rastertogridstddev(tile: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridStddev.name, Seq(tile, resolution))

    /** Bounding box STRUCT<xmin,ymin,xmax,ymax> of one H3 cell in the output CRS
     *  (`srid`, an EPSG or ESRI code; or an `outCrs` string that wins over it). */
    def gbx_h3_cell_bbox(cellid: Column): Column =
        ColumnAdapter(RST_H3_CellBBox.name, Seq(cellid, lit(4326), lit("centroids"), lit(0)))
    def gbx_h3_cell_bbox(cellid: Column, srid: Column, mode: Column, kringPad: Column): Column =
        ColumnAdapter(RST_H3_CellBBox.name, Seq(cellid, srid, mode, kringPad))
    def gbx_h3_cell_bbox(cellid: Column, srid: Int, mode: String, kringPad: Int): Column =
        gbx_h3_cell_bbox(cellid, lit(srid), lit(mode), lit(kringPad))
    /** Output-CRS-string overload: `outCrs` (EPSG:x / ESRI:x / WKT) wins over `srid`. */
    def gbx_h3_cell_bbox(
        cellid: Column, srid: Column, mode: Column, kringPad: Column, outCrs: Column
    ): Column =
        ColumnAdapter(RST_H3_CellBBox.name, Seq(cellid, srid, mode, kringPad, outCrs))

    // Operations
    def rst_asformat(tile: Column, newFormat: Column): Column = ColumnAdapter(RST_AsFormat.name, Seq(tile, newFormat))
    def rst_clip(tile: Column, clip: Column, cutlineAllTouched: Column): Column =
        ColumnAdapter(RST_Clip.name, Seq(tile, clip, cutlineAllTouched))
    def rst_combineavg(tiles: Column): Column = ColumnAdapter(RST_CombineAvg.name, Seq(tiles))
    def rst_convolve(tile: Column, kernel: Column): Column = ColumnAdapter(RST_Convolve.name, Seq(tile, kernel))
    def rst_derivedband(tile: Column, pyfunc: String, funcName: String): Column =
        ColumnAdapter(RST_DerivedBand.name, Seq(tile, lit(pyfunc), lit(funcName)))
//    def rst_dtmfromgeoms(geometries: Column, pixelSize: Column, extent: Column): Column =
//        ColumnAdapter(RST_DTMFromGeoms.name, Seq(geometries, pixelSize, extent))
    def rst_filter(tile: Column, kernelSize: Column, operation: Column): Column =
        ColumnAdapter(RST_Filter.name, Seq(tile, kernelSize, operation))
    def rst_initnodata(tile: Column): Column = ColumnAdapter(RST_InitNoData.name, Seq(tile))
    def rst_isempty(tile: Column): Column = ColumnAdapter(RST_IsEmpty.name, Seq(tile))
    def rst_mapalgebra(tiles: Column, expression: Column): Column = ColumnAdapter(RST_MapAlgebra.name, Seq(tiles, expression))
    def rst_merge(tiles: Column): Column = ColumnAdapter(RST_Merge.name, Seq(tiles))
    def rst_ndvi(tile: Column, redBand: Column, nirBand: Column): Column = ColumnAdapter(RST_NDVI.name, Seq(tile, redBand, nirBand))
    def rst_rastertoworldcoord(tile: Column, pixelX: Column, pixelY: Column): Column =
        ColumnAdapter(RST_RasterToWorldCoord.name, Seq(tile, pixelX, pixelY))
    def rst_rastertoworldcoordx(tile: Column, pixelX: Column, pixelY: Column): Column =
        ColumnAdapter(RST_RasterToWorldCoordX.name, Seq(tile, pixelX, pixelY))
    def rst_rastertoworldcoordy(tile: Column, pixelX: Column, pixelY: Column): Column =
        ColumnAdapter(RST_RasterToWorldCoordY.name, Seq(tile, pixelX, pixelY))
    def rst_transform(tile: Column, targetSrid: Column): Column = ColumnAdapter(RST_Transform.name, Seq(tile, targetSrid))
    def rst_transformcrs(tile: Column, crs: Column): Column = ColumnAdapter(RST_TransformCrs.name, Seq(tile, crs))
    def rst_transformcrs(tile: Column, crs: String): Column = rst_transformcrs(tile, lit(crs))
    def rst_tryopen(tile: Column): Column = ColumnAdapter(RST_TryOpen.name, Seq(tile))
    def rst_updatetype(tile: Column, newType: Column): Column = ColumnAdapter(RST_UpdateType.name, Seq(tile, newType))
    def rst_worldtorastercoord(tile: Column, worldX: Column, worldY: Column): Column =
        ColumnAdapter(RST_WorldToRasterCoord.name, Seq(tile, worldX, worldY))
    def rst_worldtorastercoordx(tile: Column, worldX: Column, worldY: Column): Column =
        ColumnAdapter(RST_WorldToRasterCoordX.name, Seq(tile, worldX, worldY))
    def rst_worldtorastercoordy(tile: Column, worldX: Column, worldY: Column): Column =
        ColumnAdapter(RST_WorldToRasterCoordY.name, Seq(tile, worldX, worldY))

    // Scalar-literal overloads — so users can pass plain values for non-Column params
    // (e.g. rst_clip(tile, clip, true) instead of rst_clip(tile, clip, lit(true))).
    // Column params (tile, geometry, kernel, tiles, content) stay as Column.
    def rst_bandmetadata(tile: Column, band: Int): Column = rst_bandmetadata(tile, lit(band))
    def rst_getsubdataset(tile: Column, subsetName: String): Column = rst_getsubdataset(tile, lit(subsetName))
    def rst_fromcontent(content: Column, driver: String): Column = rst_fromcontent(content, lit(driver))
    // rst_fromfile is lightweight-only (Python UDF); no Scala/JVM column helper or scalar
    // overloads (the JVM cannot read UC Volumes -- see register/#34). Use the Python/SQL binding.
    def rst_h3_tessellate(tile: Column, resolution: Int): Column = rst_h3_tessellate(tile, lit(resolution))
    def rst_h3_tessellate(tile: Column, resolution: Int, mode: String): Column =
        rst_h3_tessellate(tile, lit(resolution), mode)
    def rst_quadbin_tessellate(tile: Column, resolution: Int): Column = rst_quadbin_tessellate(tile, lit(resolution))
    def rst_quadbin_tessellate(tile: Column, resolution: Int, mode: String): Column =
        rst_quadbin_tessellate(tile, lit(resolution), mode)
    // BNG resolution accepts an Int index (±1..±6) or a String key ("1km", "100m", ...).
    def rst_bng_tessellate(tile: Column, resolution: Int): Column = rst_bng_tessellate(tile, lit(resolution))
    def rst_bng_tessellate(tile: Column, resolution: Int, mode: String): Column =
        rst_bng_tessellate(tile, lit(resolution), mode)
    def rst_bng_tessellate(tile: Column, resolution: String): Column = rst_bng_tessellate(tile, lit(resolution))
    def rst_bng_tessellate(tile: Column, resolution: String, mode: String): Column =
        rst_bng_tessellate(tile, lit(resolution), mode)
    def rst_maketiles(tile: Column, sizeInMB: Int): Column =
        rst_maketiles(tile, lit(sizeInMB))
    def rst_retile(tile: Column, tileWidth: Int, tileHeight: Int): Column =
        rst_retile(tile, lit(tileWidth), lit(tileHeight))
    def rst_tooverlappingtiles(tile: Column, tileWidth: Int, tileHeight: Int, overlap: Int): Column =
        rst_tooverlappingtiles(tile, lit(tileWidth), lit(tileHeight), lit(overlap))
    def rst_h3_rastertogridavg(tile: Column, resolution: Int): Column = rst_h3_rastertogridavg(tile, lit(resolution))
    def rst_h3_rastertogridcount(tile: Column, resolution: Int): Column = rst_h3_rastertogridcount(tile, lit(resolution))
    def rst_h3_rastertogridmax(tile: Column, resolution: Int): Column = rst_h3_rastertogridmax(tile, lit(resolution))
    def rst_h3_rastertogridmin(tile: Column, resolution: Int): Column = rst_h3_rastertogridmin(tile, lit(resolution))
    def rst_h3_rastertogridmedian(tile: Column, resolution: Int): Column = rst_h3_rastertogridmedian(tile, lit(resolution))
    def rst_h3_rastertogridsum(tile: Column, resolution: Int): Column = rst_h3_rastertogridsum(tile, lit(resolution))
    def rst_h3_rastertogridvariance(tile: Column, resolution: Int): Column = rst_h3_rastertogridvariance(tile, lit(resolution))
    def rst_h3_rastertogridstddev(tile: Column, resolution: Int): Column = rst_h3_rastertogridstddev(tile, lit(resolution))
    def rst_quadbin_rastertogridavg(tile: Column, resolution: Int): Column = rst_quadbin_rastertogridavg(tile, lit(resolution))
    def rst_quadbin_rastertogridcount(tile: Column, resolution: Int): Column = rst_quadbin_rastertogridcount(tile, lit(resolution))
    def rst_quadbin_rastertogridmax(tile: Column, resolution: Int): Column = rst_quadbin_rastertogridmax(tile, lit(resolution))
    def rst_quadbin_rastertogridmin(tile: Column, resolution: Int): Column = rst_quadbin_rastertogridmin(tile, lit(resolution))
    def rst_quadbin_rastertogridmedian(tile: Column, resolution: Int): Column = rst_quadbin_rastertogridmedian(tile, lit(resolution))
    def rst_quadbin_rastertogridsum(tile: Column, resolution: Int): Column = rst_quadbin_rastertogridsum(tile, lit(resolution))
    def rst_quadbin_rastertogridvariance(tile: Column, resolution: Int): Column = rst_quadbin_rastertogridvariance(tile, lit(resolution))
    def rst_quadbin_rastertogridstddev(tile: Column, resolution: Int): Column = rst_quadbin_rastertogridstddev(tile, lit(resolution))
    // BNG reducer resolution accepts an Int index (±1..±6) or a String key ("1km", "100m", ...).
    def rst_bng_rastertogridavg(tile: Column, resolution: Int): Column = rst_bng_rastertogridavg(tile, lit(resolution))
    def rst_bng_rastertogridcount(tile: Column, resolution: Int): Column = rst_bng_rastertogridcount(tile, lit(resolution))
    def rst_bng_rastertogridmax(tile: Column, resolution: Int): Column = rst_bng_rastertogridmax(tile, lit(resolution))
    def rst_bng_rastertogridmin(tile: Column, resolution: Int): Column = rst_bng_rastertogridmin(tile, lit(resolution))
    def rst_bng_rastertogridmedian(tile: Column, resolution: Int): Column = rst_bng_rastertogridmedian(tile, lit(resolution))
    def rst_bng_rastertogridsum(tile: Column, resolution: Int): Column = rst_bng_rastertogridsum(tile, lit(resolution))
    def rst_bng_rastertogridvariance(tile: Column, resolution: Int): Column = rst_bng_rastertogridvariance(tile, lit(resolution))
    def rst_bng_rastertogridstddev(tile: Column, resolution: Int): Column = rst_bng_rastertogridstddev(tile, lit(resolution))
    def rst_bng_rastertogridavg(tile: Column, resolution: String): Column = rst_bng_rastertogridavg(tile, lit(resolution))
    def rst_bng_rastertogridcount(tile: Column, resolution: String): Column = rst_bng_rastertogridcount(tile, lit(resolution))
    def rst_bng_rastertogridmax(tile: Column, resolution: String): Column = rst_bng_rastertogridmax(tile, lit(resolution))
    def rst_bng_rastertogridmin(tile: Column, resolution: String): Column = rst_bng_rastertogridmin(tile, lit(resolution))
    def rst_bng_rastertogridmedian(tile: Column, resolution: String): Column = rst_bng_rastertogridmedian(tile, lit(resolution))
    def rst_bng_rastertogridsum(tile: Column, resolution: String): Column = rst_bng_rastertogridsum(tile, lit(resolution))
    def rst_bng_rastertogridvariance(tile: Column, resolution: String): Column = rst_bng_rastertogridvariance(tile, lit(resolution))
    def rst_bng_rastertogridstddev(tile: Column, resolution: String): Column = rst_bng_rastertogridstddev(tile, lit(resolution))
    def rst_asformat(tile: Column, newFormat: String): Column = rst_asformat(tile, lit(newFormat))
    def rst_clip(tile: Column, clip: Column, cutlineAllTouched: Boolean): Column =
        rst_clip(tile, clip, lit(cutlineAllTouched))
    def rst_filter(tile: Column, kernelSize: Int, operation: String): Column =
        rst_filter(tile, lit(kernelSize), lit(operation))
    def rst_mapalgebra(tiles: Column, expression: String): Column = rst_mapalgebra(tiles, lit(expression))
    def rst_ndvi(tile: Column, redBand: Int, nirBand: Int): Column = rst_ndvi(tile, lit(redBand), lit(nirBand))
    def rst_rastertoworldcoord(tile: Column, pixelX: Int, pixelY: Int): Column =
        rst_rastertoworldcoord(tile, lit(pixelX), lit(pixelY))
    def rst_rastertoworldcoordx(tile: Column, pixelX: Int, pixelY: Int): Column =
        rst_rastertoworldcoordx(tile, lit(pixelX), lit(pixelY))
    def rst_rastertoworldcoordy(tile: Column, pixelX: Int, pixelY: Int): Column =
        rst_rastertoworldcoordy(tile, lit(pixelX), lit(pixelY))
    def rst_transform(tile: Column, targetSrid: Int): Column = rst_transform(tile, lit(targetSrid))
    def rst_updatetype(tile: Column, newType: String): Column = rst_updatetype(tile, lit(newType))
    def rst_worldtorastercoord(tile: Column, worldX: Double, worldY: Double): Column =
        rst_worldtorastercoord(tile, lit(worldX), lit(worldY))
    def rst_worldtorastercoordx(tile: Column, worldX: Double, worldY: Double): Column =
        rst_worldtorastercoordx(tile, lit(worldX), lit(worldY))
    def rst_worldtorastercoordy(tile: Column, worldX: Double, worldY: Double): Column =
        rst_worldtorastercoordy(tile, lit(worldX), lit(worldY))

    // Web-mercator tile output (Column form)
    def rst_to_webmercator(tile: Column): Column =
        ColumnAdapter(RST_ToWebMercator.name, Seq(tile, lit("bilinear")))
    def rst_to_webmercator(tile: Column, resampling: Column): Column =
        ColumnAdapter(RST_ToWebMercator.name, Seq(tile, resampling))
    def rst_to_webmercator(tile: Column, resampling: String): Column =
        rst_to_webmercator(tile, lit(resampling))

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

    def rst_xyzpyramid(tile: Column, minZ: Column, maxZ: Column): Column =
        ColumnAdapter(RST_XYZPyramid.name, Seq(tile, minZ, maxZ, lit("PNG"), lit(256), lit("bilinear")))
    def rst_xyzpyramid(
        tile: Column, minZ: Column, maxZ: Column,
        format: Column, size: Column, resampling: Column
    ): Column =
        ColumnAdapter(RST_XYZPyramid.name, Seq(tile, minZ, maxZ, format, size, resampling))
    def rst_xyzpyramid(tile: Column, minZ: Int, maxZ: Int): Column =
        rst_xyzpyramid(tile, lit(minZ), lit(maxZ))
    def rst_xyzpyramid(
        tile: Column, minZ: Int, maxZ: Int,
        format: String, size: Int, resampling: String
    ): Column =
        rst_xyzpyramid(tile, lit(minZ), lit(maxZ), lit(format), lit(size), lit(resampling))

    // Vector<->raster bridge (Column form)
    def rst_rasterize(
        geom: Column, value: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        widthPx: Column, heightPx: Column, out_srid: Column
    ): Column =
        ColumnAdapter(RST_Rasterize.name, Seq(geom, value, xmin, ymin, xmax, ymax, widthPx, heightPx, out_srid))

    def rst_polygonize(tile: Column): Column =
        ColumnAdapter(RST_Polygonize.name, Seq(tile, lit(1), lit(4)))
    def rst_polygonize(tile: Column, band: Column): Column =
        ColumnAdapter(RST_Polygonize.name, Seq(tile, band, lit(4)))
    def rst_polygonize(tile: Column, band: Column, connectedness: Column): Column =
        ColumnAdapter(RST_Polygonize.name, Seq(tile, band, connectedness))

    // Terrain analysis (DEM processing) - Column form
    def rst_slope(tile: Column): Column =
        ColumnAdapter(RST_Slope.name, Seq(tile, lit("degrees"), lit(Double.NaN), lit(Double.NaN)))
    def rst_slope(tile: Column, unit: Column): Column =
        ColumnAdapter(RST_Slope.name, Seq(tile, unit, lit(Double.NaN), lit(Double.NaN)))
    def rst_slope(tile: Column, unit: Column, xscale: Column, yscale: Column): Column =
        ColumnAdapter(RST_Slope.name, Seq(tile, unit, xscale, yscale))
    def rst_slope(tile: Column, unit: String): Column = rst_slope(tile, lit(unit))
    def rst_slope(tile: Column, unit: String, xscale: Double, yscale: Double): Column =
        rst_slope(tile, lit(unit), lit(xscale), lit(yscale))

    def rst_aspect(tile: Column): Column =
        ColumnAdapter(RST_Aspect.name, Seq(tile, lit(false), lit(false)))
    def rst_aspect(tile: Column, trigonometric: Column): Column =
        ColumnAdapter(RST_Aspect.name, Seq(tile, trigonometric, lit(false)))
    def rst_aspect(tile: Column, trigonometric: Column, zeroForFlat: Column): Column =
        ColumnAdapter(RST_Aspect.name, Seq(tile, trigonometric, zeroForFlat))
    def rst_aspect(tile: Column, trigonometric: Boolean): Column =
        rst_aspect(tile, lit(trigonometric))
    def rst_aspect(tile: Column, trigonometric: Boolean, zeroForFlat: Boolean): Column =
        rst_aspect(tile, lit(trigonometric), lit(zeroForFlat))

    def rst_hillshade(tile: Column): Column =
        ColumnAdapter(RST_Hillshade.name, Seq(tile, lit(315.0), lit(45.0), lit(1.0)))
    def rst_hillshade(tile: Column, azimuth: Column, altitude: Column, zFactor: Column): Column =
        ColumnAdapter(RST_Hillshade.name, Seq(tile, azimuth, altitude, zFactor))
    def rst_hillshade(tile: Column, azimuth: Double, altitude: Double): Column =
        rst_hillshade(tile, lit(azimuth), lit(altitude), lit(1.0))
    def rst_hillshade(tile: Column, azimuth: Double, altitude: Double, zFactor: Double): Column =
        rst_hillshade(tile, lit(azimuth), lit(altitude), lit(zFactor))

    def rst_tri(tile: Column): Column = ColumnAdapter(RST_TRI.name, Seq(tile))
    def rst_tpi(tile: Column): Column = ColumnAdapter(RST_TPI.name, Seq(tile))
    def rst_roughness(tile: Column): Column = ColumnAdapter(RST_Roughness.name, Seq(tile))

    def rst_color_relief(tile: Column, colorTablePath: Column): Column =
        ColumnAdapter(RST_ColorRelief.name, Seq(tile, colorTablePath))
    def rst_color_relief(tile: Column, colorTablePath: String): Column =
        rst_color_relief(tile, lit(colorTablePath))

    // Spectral indices (Wave 8b) - all delegate to RST_MapAlgebra under the hood.
    def rst_evi(
        tile: Column, redIdx: Column, nirIdx: Column, blueIdx: Column
    ): Column =
        ColumnAdapter(RST_EVI.name, Seq(tile, redIdx, nirIdx, blueIdx,
            lit(1.0), lit(6.0), lit(7.5), lit(2.5)))
    def rst_evi(
        tile: Column, redIdx: Column, nirIdx: Column, blueIdx: Column,
        l: Column, c1: Column, c2: Column, g: Column
    ): Column =
        ColumnAdapter(RST_EVI.name, Seq(tile, redIdx, nirIdx, blueIdx, l, c1, c2, g))
    def rst_evi(tile: Column, redIdx: Int, nirIdx: Int, blueIdx: Int): Column =
        rst_evi(tile, lit(redIdx), lit(nirIdx), lit(blueIdx))
    def rst_evi(
        tile: Column, redIdx: Int, nirIdx: Int, blueIdx: Int,
        l: Double, c1: Double, c2: Double, g: Double
    ): Column =
        rst_evi(tile, lit(redIdx), lit(nirIdx), lit(blueIdx), lit(l), lit(c1), lit(c2), lit(g))

    def rst_savi(tile: Column, redIdx: Column, nirIdx: Column): Column =
        ColumnAdapter(RST_SAVI.name, Seq(tile, redIdx, nirIdx, lit(0.5)))
    def rst_savi(tile: Column, redIdx: Column, nirIdx: Column, l: Column): Column =
        ColumnAdapter(RST_SAVI.name, Seq(tile, redIdx, nirIdx, l))
    def rst_savi(tile: Column, redIdx: Int, nirIdx: Int): Column =
        rst_savi(tile, lit(redIdx), lit(nirIdx))
    def rst_savi(tile: Column, redIdx: Int, nirIdx: Int, l: Double): Column =
        rst_savi(tile, lit(redIdx), lit(nirIdx), lit(l))

    def rst_ndwi(tile: Column, greenIdx: Column, nirIdx: Column): Column =
        ColumnAdapter(RST_NDWI.name, Seq(tile, greenIdx, nirIdx))
    def rst_ndwi(tile: Column, greenIdx: Int, nirIdx: Int): Column =
        rst_ndwi(tile, lit(greenIdx), lit(nirIdx))

    def rst_nbr(tile: Column, nirIdx: Column, swirIdx: Column): Column =
        ColumnAdapter(RST_NBR.name, Seq(tile, nirIdx, swirIdx))
    def rst_nbr(tile: Column, nirIdx: Int, swirIdx: Int): Column =
        rst_nbr(tile, lit(nirIdx), lit(swirIdx))

    def rst_index(tile: Column, formulaName: Column, bandMap: Column): Column =
        ColumnAdapter(RST_Index.name, Seq(tile, formulaName, bandMap))
    def rst_index(tile: Column, formulaName: String, bandMap: Column): Column =
        rst_index(tile, lit(formulaName), bandMap)

    // Resample family - gdal.Warp -tr / -ts wrappers
    def rst_resample(tile: Column, factor: Column): Column =
        ColumnAdapter(RST_Resample.name, Seq(tile, factor, lit("bilinear")))
    def rst_resample(tile: Column, factor: Column, algorithm: Column): Column =
        ColumnAdapter(RST_Resample.name, Seq(tile, factor, algorithm))
    def rst_resample(tile: Column, factor: Double): Column =
        rst_resample(tile, lit(factor))
    def rst_resample(tile: Column, factor: Double, algorithm: String): Column =
        rst_resample(tile, lit(factor), lit(algorithm))

    def rst_resample_to_size(tile: Column, widthPx: Column, heightPx: Column): Column =
        ColumnAdapter(RST_ResampleToSize.name, Seq(tile, widthPx, heightPx, lit("bilinear")))
    def rst_resample_to_size(tile: Column, widthPx: Column, heightPx: Column, algorithm: Column): Column =
        ColumnAdapter(RST_ResampleToSize.name, Seq(tile, widthPx, heightPx, algorithm))
    def rst_resample_to_size(tile: Column, widthPx: Int, heightPx: Int): Column =
        rst_resample_to_size(tile, lit(widthPx), lit(heightPx))
    def rst_resample_to_size(tile: Column, widthPx: Int, heightPx: Int, algorithm: String): Column =
        rst_resample_to_size(tile, lit(widthPx), lit(heightPx), lit(algorithm))

    def rst_resample_to_res(tile: Column, xRes: Column, yRes: Column): Column =
        ColumnAdapter(RST_ResampleToRes.name, Seq(tile, xRes, yRes, lit("bilinear")))
    def rst_resample_to_res(tile: Column, xRes: Column, yRes: Column, algorithm: Column): Column =
        ColumnAdapter(RST_ResampleToRes.name, Seq(tile, xRes, yRes, algorithm))
    def rst_resample_to_res(tile: Column, xRes: Double, yRes: Double): Column =
        rst_resample_to_res(tile, lit(xRes), lit(yRes))
    def rst_resample_to_res(tile: Column, xRes: Double, yRes: Double, algorithm: String): Column =
        rst_resample_to_res(tile, lit(xRes), lit(yRes), lit(algorithm))

    // IDW interpolation - non-aggregator (arrays in a single row)
    def rst_gridfrompoints(
        points: Column, values: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        widthPx: Column, heightPx: Column, out_srid: Column
    ): Column =
        ColumnAdapter(RST_GridFromPoints.name, Seq(
            points, values, xmin, ymin, xmax, ymax, widthPx, heightPx, out_srid,
            lit(RST_GridFromPoints.DefaultPower),
            lit(RST_GridFromPoints.DefaultMaxPoints)
        ))
    def rst_gridfrompoints(
        points: Column, values: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        widthPx: Column, heightPx: Column, out_srid: Column,
        power: Column, maxPts: Column
    ): Column =
        ColumnAdapter(RST_GridFromPoints.name, Seq(
            points, values, xmin, ymin, xmax, ymax, widthPx, heightPx, out_srid, power, maxPts
        ))

    // IDW interpolation - aggregator (one point/value per row)
    def rst_gridfrompoints_agg(
        point: Column, value: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        widthPx: Column, heightPx: Column, out_srid: Column
    ): Column =
        ColumnAdapter(RST_GridFromPointsAgg.name, Seq(
            point, value, xmin, ymin, xmax, ymax, widthPx, heightPx, out_srid,
            lit(RST_GridFromPoints.DefaultPower),
            lit(RST_GridFromPoints.DefaultMaxPoints)
        ))
    def rst_gridfrompoints_agg(
        point: Column, value: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        widthPx: Column, heightPx: Column, out_srid: Column,
        power: Column, maxPts: Column
    ): Column =
        ColumnAdapter(RST_GridFromPointsAgg.name, Seq(
            point, value, xmin, ymin, xmax, ymax, widthPx, heightPx, out_srid, power, maxPts
        ))

    // Pixel ops + extraction — Column form + scalar overloads
    def rst_fillnodata(tile: Column): Column =
        ColumnAdapter(RST_FillNodata.name, Seq(tile, lit(100.0), lit(0)))
    def rst_fillnodata(tile: Column, maxSearchDist: Column): Column =
        ColumnAdapter(RST_FillNodata.name, Seq(tile, maxSearchDist, lit(0)))
    def rst_fillnodata(tile: Column, maxSearchDist: Column, smoothingIter: Column): Column =
        ColumnAdapter(RST_FillNodata.name, Seq(tile, maxSearchDist, smoothingIter))
    def rst_fillnodata(tile: Column, maxSearchDist: Double): Column =
        rst_fillnodata(tile, lit(maxSearchDist))
    def rst_fillnodata(tile: Column, maxSearchDist: Double, smoothingIter: Int): Column =
        rst_fillnodata(tile, lit(maxSearchDist), lit(smoothingIter))

    def rst_sample(tile: Column, geom: Column): Column =
        ColumnAdapter(RST_Sample.name, Seq(tile, geom))

    def rst_setsrid(tile: Column, srid: Column): Column =
        ColumnAdapter(RST_SetSrid.name, Seq(tile, srid))
    def rst_setsrid(tile: Column, srid: Int): Column =
        rst_setsrid(tile, lit(srid))

    def rst_setcrs(tile: Column, crs: Column): Column =
        ColumnAdapter(RST_SetCrs.name, Seq(tile, crs))
    def rst_setcrs(tile: Column, crs: String): Column =
        rst_setcrs(tile, lit(crs))

    // NaN (not null) is the "derive from band stats" sentinel for min/max: a null primitive-Double
    // literal is force-null-checked by Spark's Invoke and short-circuits the whole result to null
    // before eval runs (this silently broke rst_histogram(tile)). eval maps NaN -> None. Mirrors the
    // NaN default in RST_Histogram.builder() for the SQL path.
    def rst_histogram(tile: Column): Column =
        ColumnAdapter(RST_Histogram.name, Seq(
            tile, lit(256), lit(Double.NaN), lit(Double.NaN), lit(false)
        ))
    def rst_histogram(tile: Column, nBuckets: Column): Column =
        ColumnAdapter(RST_Histogram.name, Seq(
            tile, nBuckets, lit(Double.NaN), lit(Double.NaN), lit(false)
        ))
    def rst_histogram(tile: Column, nBuckets: Column, minVal: Column, maxVal: Column): Column =
        ColumnAdapter(RST_Histogram.name, Seq(
            tile, nBuckets, minVal, maxVal, lit(false)
        ))
    def rst_histogram(
        tile: Column, nBuckets: Column, minVal: Column, maxVal: Column, includeNodata: Column
    ): Column =
        ColumnAdapter(RST_Histogram.name, Seq(
            tile, nBuckets, minVal, maxVal, includeNodata
        ))
    def rst_histogram(tile: Column, nBuckets: Int): Column =
        rst_histogram(tile, lit(nBuckets))

    def rst_threshold(tile: Column, op: Column, value: Column): Column =
        ColumnAdapter(RST_Threshold.name, Seq(tile, op, value))
    def rst_threshold(tile: Column, op: String, value: Double): Column =
        rst_threshold(tile, lit(op), lit(value))

    def rst_buildoverviews(tile: Column, levels: Column): Column =
        ColumnAdapter(RST_BuildOverviews.name, Seq(tile, levels, lit("average")))
    def rst_buildoverviews(tile: Column, levels: Column, resampling: Column): Column =
        ColumnAdapter(RST_BuildOverviews.name, Seq(tile, levels, resampling))
    def rst_buildoverviews(tile: Column, levels: Array[Int]): Column =
        rst_buildoverviews(tile, lit(levels))
    def rst_buildoverviews(tile: Column, levels: Array[Int], resampling: String): Column =
        rst_buildoverviews(tile, lit(levels), lit(resampling))

    def rst_band(tile: Column, bandIndex: Column): Column =
        ColumnAdapter(RST_Band.name, Seq(tile, bandIndex))
    def rst_band(tile: Column, bandIndex: Int): Column =
        rst_band(tile, lit(bandIndex))

    // Analysis (COG / proximity / contour / viewshed) — Column form + scalar overloads
    def rst_cog_convert(tile: Column): Column =
        ColumnAdapter(RST_CogConvert.name, Seq(tile, lit("DEFLATE"), lit(512), lit("AVERAGE")))
    def rst_cog_convert(tile: Column, compression: Column): Column =
        ColumnAdapter(RST_CogConvert.name, Seq(tile, compression, lit(512), lit("AVERAGE")))
    def rst_cog_convert(tile: Column, compression: Column, blocksize: Column): Column =
        ColumnAdapter(RST_CogConvert.name, Seq(tile, compression, blocksize, lit("AVERAGE")))
    def rst_cog_convert(
        tile: Column, compression: Column, blocksize: Column, overviewResampling: Column
    ): Column = ColumnAdapter(RST_CogConvert.name, Seq(tile, compression, blocksize, overviewResampling))
    def rst_cog_convert(tile: Column, compression: String): Column =
        rst_cog_convert(tile, lit(compression))
    def rst_cog_convert(tile: Column, compression: String, blocksize: Int): Column =
        rst_cog_convert(tile, lit(compression), lit(blocksize))
    def rst_cog_convert(
        tile: Column, compression: String, blocksize: Int, overviewResampling: String
    ): Column = rst_cog_convert(tile, lit(compression), lit(blocksize), lit(overviewResampling))

    // NaN (not null) is the "unlimited" sentinel for max_distance: a null primitive-Double literal is
    // force-null-checked by Spark's Invoke and short-circuits the whole result to null before eval
    // runs (this silently broke rst_proximity(tile)). eval maps NaN -> None. target_values stays a
    // nullable string (object type — not force-null-checked). Mirrors RST_Proximity.builder().
    def rst_proximity(tile: Column): Column =
        ColumnAdapter(RST_Proximity.name, Seq(
            tile, lit(null).cast("string"), lit("GEO"), lit(Double.NaN)
        ))
    def rst_proximity(tile: Column, targetValues: Column): Column =
        ColumnAdapter(RST_Proximity.name, Seq(
            tile, targetValues, lit("GEO"), lit(Double.NaN)
        ))
    def rst_proximity(tile: Column, targetValues: Column, distUnits: Column): Column =
        ColumnAdapter(RST_Proximity.name, Seq(
            tile, targetValues, distUnits, lit(Double.NaN)
        ))
    def rst_proximity(
        tile: Column, targetValues: Column, distUnits: Column, maxDistance: Column
    ): Column = ColumnAdapter(RST_Proximity.name, Seq(tile, targetValues, distUnits, maxDistance))

    def rst_contour(tile: Column, levels: Column): Column =
        ColumnAdapter(RST_Contour.name, Seq(tile, levels, lit(0.0), lit(0.0), lit("elev")))
    def rst_contour(tile: Column, levels: Column, interval: Column): Column =
        ColumnAdapter(RST_Contour.name, Seq(tile, levels, interval, lit(0.0), lit("elev")))
    def rst_contour(
        tile: Column, levels: Column, interval: Column, base: Column
    ): Column = ColumnAdapter(RST_Contour.name, Seq(tile, levels, interval, base, lit("elev")))
    def rst_contour(
        tile: Column, levels: Column, interval: Column, base: Column, attrField: Column
    ): Column = ColumnAdapter(RST_Contour.name, Seq(tile, levels, interval, base, attrField))

    // NaN (not null) is the "unlimited" sentinel for max_distance: a null primitive-Double literal is
    // force-null-checked by Spark's Invoke and short-circuits the whole result to null before eval
    // runs. eval maps NaN -> None. Mirrors RST_Viewshed.builder().
    def rst_viewshed(tile: Column, observerGeom: Column, observerHeight: Column): Column =
        ColumnAdapter(RST_Viewshed.name, Seq(
            tile, observerGeom, observerHeight, lit(1.6), lit(Double.NaN)
        ))
    def rst_viewshed(
        tile: Column, observerGeom: Column, observerHeight: Column, targetHeight: Column
    ): Column = ColumnAdapter(RST_Viewshed.name, Seq(
        tile, observerGeom, observerHeight, targetHeight, lit(Double.NaN)
    ))
    def rst_viewshed(
        tile: Column, observerGeom: Column, observerHeight: Column,
        targetHeight: Column, maxDistance: Column
    ): Column = ColumnAdapter(RST_Viewshed.name, Seq(
        tile, observerGeom, observerHeight, targetHeight, maxDistance
    ))

}
