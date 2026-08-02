package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.expressions.{ExpressionConfig, RegistryDelegate}
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

        val expressionConfig = ExpressionConfig(spark)

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
        // pyrx), accessible from Python and SQL when geobrix[light] is installed. Issue #34.

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
        rd.register(RST_Threshold)

        // Analysis (COG / proximity / contour / viewshed — GDAL wrappers)
        rd.register(RST_CogConvert)
        rd.register(RST_Contour)
        rd.register(RST_Proximity)
        rd.register(RST_Viewshed)

        sc.getConf.set(flag, "true")
    }

    // Accessors
    def rst_avg(tileExpr: Column): Column = ColumnAdapter(RST_Avg.name, Seq(tileExpr))
    def rst_bandmetadata(tileExpr: Column, band: Column): Column = ColumnAdapter(RST_BandMetaData.name, Seq(tileExpr, band))
    def rst_boundingbox(tileExpr: Column): Column = ColumnAdapter(RST_BoundingBox.name, Seq(tileExpr))
    def rst_format(tileExpr: Column): Column = ColumnAdapter(RST_Format.name, Seq(tileExpr))
    def rst_georeference(tileExpr: Column): Column = ColumnAdapter(RST_GeoReference.name, Seq(tileExpr))
    def rst_getnodata(tileExpr: Column): Column = ColumnAdapter(RST_GetNoData.name, Seq(tileExpr))
    def rst_getsubdataset(tileExpr: Column, subsetName: Column): Column = ColumnAdapter(RST_GetSubdataset.name, Seq(tileExpr, subsetName))
    def rst_height(tileExpr: Column): Column = ColumnAdapter(RST_Height.name, Seq(tileExpr))
    def rst_max(tileExpr: Column): Column = ColumnAdapter(RST_Max.name, Seq(tileExpr))
    def rst_median(tileExpr: Column): Column = ColumnAdapter(RST_Median.name, Seq(tileExpr))
    def rst_memsize(tileExpr: Column): Column = ColumnAdapter(RST_MemSize.name, Seq(tileExpr))
    def rst_metadata(tileExpr: Column): Column = ColumnAdapter(RST_MetaData.name, Seq(tileExpr))
    def rst_min(tileExpr: Column): Column = ColumnAdapter(RST_Min.name, Seq(tileExpr))
    def rst_numbands(tileExpr: Column): Column = ColumnAdapter(RST_NumBands.name, Seq(tileExpr))
    def rst_pixelcount(tileExpr: Column): Column = ColumnAdapter(RST_PixelCount.name, Seq(tileExpr))
    def rst_pixelheight(tileExpr: Column): Column = ColumnAdapter(RST_PixelHeight.name, Seq(tileExpr))
    def rst_pixelwidth(tileExpr: Column): Column = ColumnAdapter(RST_PixelWidth.name, Seq(tileExpr))
    def rst_rotation(tileExpr: Column): Column = ColumnAdapter(RST_Rotation.name, Seq(tileExpr))
    def rst_scalex(tileExpr: Column): Column = ColumnAdapter(RST_ScaleX.name, Seq(tileExpr))
    def rst_scaley(tileExpr: Column): Column = ColumnAdapter(RST_ScaleY.name, Seq(tileExpr))
    def rst_skewx(tileExpr: Column): Column = ColumnAdapter(RST_SkewX.name, Seq(tileExpr))
    def rst_skewy(tileExpr: Column): Column = ColumnAdapter(RST_SkewY.name, Seq(tileExpr))
    def rst_srid(tileExpr: Column): Column = ColumnAdapter(RST_SRID.name, Seq(tileExpr))
    def rst_subdatasets(tileExpr: Column): Column = ColumnAdapter(RST_Subdatasets.name, Seq(tileExpr))
    def rst_summary(tileExpr: Column): Column = ColumnAdapter(RST_Summary.name, Seq(tileExpr))
    def rst_type(tileExpr: Column): Column = ColumnAdapter(RST_Type.name, Seq(tileExpr))
    def rst_upperleftx(tileExpr: Column): Column = ColumnAdapter(RST_UpperLeftX.name, Seq(tileExpr))
    def rst_upperlefty(tileExpr: Column): Column = ColumnAdapter(RST_UpperLeftY.name, Seq(tileExpr))
    def rst_width(tileExpr: Column): Column = ColumnAdapter(RST_Width.name, Seq(tileExpr))

    // Aggregators
def rst_combineavg_agg(tileExpr: Column): Column = ColumnAdapter(RST_CombineAvgAgg.name, Seq(tileExpr))
    def rst_derivedband_agg(tileExpr: Column, pyfunc: String, funcName: String): Column =
      ColumnAdapter(RST_DerivedBandAgg.name, Seq(tileExpr, lit(pyfunc), lit(funcName)))
    def rst_merge_agg(tileExpr: Column): Column = ColumnAdapter(RST_MergeAgg.name, Seq(tileExpr))

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
        cellid: Column, value: Column, srid: Column, pixelSize: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        width: Column, height: Column, mode: Column, kringPad: Column
    ): Column =
        ColumnAdapter(RST_H3_RasterizeAgg.name, Seq(
            cellid, value, srid, pixelSize, xmin, ymin, xmax, ymax, width, height, mode, kringPad
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
        cellid: Column, value: Column, srid: Column, pixelSize: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        width: Column, height: Column, mode: Column, kringPad: Column
    ): Column =
        ColumnAdapter(RST_Quadbin_RasterizeAgg.name, Seq(
            cellid, value, srid, pixelSize, xmin, ymin, xmax, ymax, width, height, mode, kringPad
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
        cellid: Column, value: Column, srid: Column, pixelSize: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        width: Column, height: Column, mode: Column, kringPad: Column
    ): Column =
        ColumnAdapter(RST_BNG_RasterizeAgg.name, Seq(
            cellid, value, srid, pixelSize, xmin, ymin, xmax, ymax, width, height, mode, kringPad
        ))

    // Constructors
    def rst_fromcontent(content: Column, driver: Column): Column = ColumnAdapter(RST_FromContent.name, Seq(content, driver))
    // rst_fromfile is lightweight-only (Python UDF); no Scala/JVM column helper (see register/#34).
    def rst_frombands(bands: Column): Column = ColumnAdapter(RST_FromBands.name, Seq(bands))

    // Generators
    def rst_h3_tessellate(tileExpr: Column, resolution: Column): Column = ColumnAdapter(RST_H3_Tessellate.name, Seq(tileExpr, resolution))
    def rst_h3_tessellate(tileExpr: Column, resolution: Column, mode: String): Column =
        ColumnAdapter(RST_H3_Tessellate.name, Seq(tileExpr, resolution, lit(mode)))
    def rst_quadbin_tessellate(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_Tessellate.name, Seq(tileExpr, resolution))
    def rst_quadbin_tessellate(tileExpr: Column, resolution: Column, mode: String): Column =
        ColumnAdapter(RST_Quadbin_Tessellate.name, Seq(tileExpr, resolution, lit(mode)))
    def rst_bng_tessellate(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_Tessellate.name, Seq(tileExpr, resolution))
    def rst_bng_tessellate(tileExpr: Column, resolution: Column, mode: String): Column =
        ColumnAdapter(RST_BNG_Tessellate.name, Seq(tileExpr, resolution, lit(mode)))
    def rst_maketiles(tileExpr: Column, tileWidth: Column, tileHeight: Column): Column =
        ColumnAdapter(RST_MakeTiles.name, Seq(tileExpr, tileWidth, tileHeight))
    def rst_retile(tileExpr: Column, tileWidth: Column, tileHeight: Column): Column =
        ColumnAdapter(RST_ReTile.name, Seq(tileExpr, tileWidth, tileHeight))
    def rst_separatebands(tileExpr: Column): Column = ColumnAdapter(RST_SeparateBands.name, Seq(tileExpr))
    def rst_tooverlappingtiles(tileExpr: Column, tileWidth: Column, tileHeight: Column, overlap: Column): Column =
        ColumnAdapter(RST_ToOverlappingTiles.name, Seq(tileExpr, tileWidth, tileHeight, overlap))

    // Grid
    def rst_h3_rastertogridavg(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridAvg.name, Seq(tileExpr, resolution))
    def rst_h3_rastertogridcount(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridCount.name, Seq(tileExpr, resolution))
    def rst_h3_rastertogridmax(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridMax.name, Seq(tileExpr, resolution))
    def rst_h3_rastertogridmin(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridMin.name, Seq(tileExpr, resolution))
    def rst_h3_rastertogridmedian(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridMedian.name, Seq(tileExpr, resolution))
    def rst_h3_rastertogridsum(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridSum.name, Seq(tileExpr, resolution))
    def rst_h3_rastertogridvariance(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridVariance.name, Seq(tileExpr, resolution))
    def rst_h3_rastertogridstddev(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_H3_RasterToGridStddev.name, Seq(tileExpr, resolution))
    def rst_quadbin_rastertogridavg(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridAvg.name, Seq(tileExpr, resolution))
    def rst_quadbin_rastertogridcount(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridCount.name, Seq(tileExpr, resolution))
    def rst_quadbin_rastertogridmax(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridMax.name, Seq(tileExpr, resolution))
    def rst_quadbin_rastertogridmin(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridMin.name, Seq(tileExpr, resolution))
    def rst_quadbin_rastertogridmedian(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridMedian.name, Seq(tileExpr, resolution))
    def rst_quadbin_rastertogridsum(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridSum.name, Seq(tileExpr, resolution))
    def rst_quadbin_rastertogridvariance(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridVariance.name, Seq(tileExpr, resolution))
    def rst_quadbin_rastertogridstddev(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_Quadbin_RasterToGridStddev.name, Seq(tileExpr, resolution))
    def rst_bng_rastertogridavg(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridAvg.name, Seq(tileExpr, resolution))
    def rst_bng_rastertogridcount(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridCount.name, Seq(tileExpr, resolution))
    def rst_bng_rastertogridmax(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridMax.name, Seq(tileExpr, resolution))
    def rst_bng_rastertogridmin(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridMin.name, Seq(tileExpr, resolution))
    def rst_bng_rastertogridmedian(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridMedian.name, Seq(tileExpr, resolution))
    def rst_bng_rastertogridsum(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridSum.name, Seq(tileExpr, resolution))
    def rst_bng_rastertogridvariance(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridVariance.name, Seq(tileExpr, resolution))
    def rst_bng_rastertogridstddev(tileExpr: Column, resolution: Column): Column =
        ColumnAdapter(RST_BNG_RasterToGridStddev.name, Seq(tileExpr, resolution))

    /** Bounding box STRUCT<xmin,ymin,xmax,ymax> of one H3 cell in `srid`. */
    def gbx_h3_cell_bbox(cellid: Column): Column =
        ColumnAdapter(RST_H3_CellBBox.name, Seq(cellid, lit(4326), lit("centroids"), lit(0)))
    def gbx_h3_cell_bbox(cellid: Column, srid: Column, mode: Column, kringPad: Column): Column =
        ColumnAdapter(RST_H3_CellBBox.name, Seq(cellid, srid, mode, kringPad))
    def gbx_h3_cell_bbox(cellid: Column, srid: Int, mode: String, kringPad: Int): Column =
        gbx_h3_cell_bbox(cellid, lit(srid), lit(mode), lit(kringPad))

    // Operations
    def rst_asformat(tileExpr: Column, newFormat: Column): Column = ColumnAdapter(RST_AsFormat.name, Seq(tileExpr, newFormat))
    def rst_clip(tileExpr: Column, clip: Column, cutlineAllTouched: Column): Column =
        ColumnAdapter(RST_Clip.name, Seq(tileExpr, clip, cutlineAllTouched))
    def rst_combineavg(tiles: Column): Column = ColumnAdapter(RST_CombineAvg.name, Seq(tiles))
    def rst_convolve(tileExpr: Column, kernel: Column): Column = ColumnAdapter(RST_Convolve.name, Seq(tileExpr, kernel))
    def rst_derivedband(tileExpr: Column, pyfunc: String, funcName: String): Column =
        ColumnAdapter(RST_DerivedBand.name, Seq(tileExpr, lit(pyfunc), lit(funcName)))
//    def rst_dtmfromgeoms(geometries: Column, pixelSize: Column, extent: Column): Column =
//        ColumnAdapter(RST_DTMFromGeoms.name, Seq(geometries, pixelSize, extent))
    def rst_filter(tileExpr: Column, kernelSize: Column, operation: Column): Column =
        ColumnAdapter(RST_Filter.name, Seq(tileExpr, kernelSize, operation))
    def rst_initnodata(tileExpr: Column): Column = ColumnAdapter(RST_InitNoData.name, Seq(tileExpr))
    def rst_isempty(tileExpr: Column): Column = ColumnAdapter(RST_IsEmpty.name, Seq(tileExpr))
    def rst_mapalgebra(tiles: Column, expression: Column): Column = ColumnAdapter(RST_MapAlgebra.name, Seq(tiles, expression))
    def rst_merge(tiles: Column): Column = ColumnAdapter(RST_Merge.name, Seq(tiles))
    def rst_ndvi(tileExpr: Column, redBand: Column, nirBand: Column): Column = ColumnAdapter(RST_NDVI.name, Seq(tileExpr, redBand, nirBand))
    def rst_rastertoworldcoord(tileExpr: Column, pixelX: Column, pixelY: Column): Column =
        ColumnAdapter(RST_RasterToWorldCoord.name, Seq(tileExpr, pixelX, pixelY))
    def rst_rastertoworldcoordx(tileExpr: Column, pixelX: Column, pixelY: Column): Column =
        ColumnAdapter(RST_RasterToWorldCoordX.name, Seq(tileExpr, pixelX, pixelY))
    def rst_rastertoworldcoordy(tileExpr: Column, pixelX: Column, pixelY: Column): Column =
        ColumnAdapter(RST_RasterToWorldCoordY.name, Seq(tileExpr, pixelX, pixelY))
    def rst_transform(tileExpr: Column, targetSrid: Column): Column = ColumnAdapter(RST_Transform.name, Seq(tileExpr, targetSrid))
    def rst_tryopen(tileExpr: Column): Column = ColumnAdapter(RST_TryOpen.name, Seq(tileExpr))
    def rst_updatetype(tileExpr: Column, newType: Column): Column = ColumnAdapter(RST_UpdateType.name, Seq(tileExpr, newType))
    def rst_worldtorastercoord(tileExpr: Column, worldX: Column, worldY: Column): Column =
        ColumnAdapter(RST_WorldToRasterCoord.name, Seq(tileExpr, worldX, worldY))
    def rst_worldtorastercoordx(tileExpr: Column, worldX: Column, worldY: Column): Column =
        ColumnAdapter(RST_WorldToRasterCoordX.name, Seq(tileExpr, worldX, worldY))
    def rst_worldtorastercoordy(tileExpr: Column, worldX: Column, worldY: Column): Column =
        ColumnAdapter(RST_WorldToRasterCoordY.name, Seq(tileExpr, worldX, worldY))

    // Scalar-literal overloads — so users can pass plain values for non-Column params
    // (e.g. rst_clip(tile, clip, true) instead of rst_clip(tile, clip, lit(true))).
    // Column params (tile, geometry, kernel, tiles, content) stay as Column.
    def rst_bandmetadata(tileExpr: Column, band: Int): Column = rst_bandmetadata(tileExpr, lit(band))
    def rst_getsubdataset(tileExpr: Column, subsetName: String): Column = rst_getsubdataset(tileExpr, lit(subsetName))
    def rst_fromcontent(content: Column, driver: String): Column = rst_fromcontent(content, lit(driver))
    // rst_fromfile is lightweight-only (Python UDF); no Scala/JVM column helper or scalar
    // overloads (the JVM cannot read UC Volumes -- see register/#34). Use the Python/SQL binding.
    def rst_h3_tessellate(tileExpr: Column, resolution: Int): Column = rst_h3_tessellate(tileExpr, lit(resolution))
    def rst_h3_tessellate(tileExpr: Column, resolution: Int, mode: String): Column =
        rst_h3_tessellate(tileExpr, lit(resolution), mode)
    def rst_quadbin_tessellate(tileExpr: Column, resolution: Int): Column = rst_quadbin_tessellate(tileExpr, lit(resolution))
    def rst_quadbin_tessellate(tileExpr: Column, resolution: Int, mode: String): Column =
        rst_quadbin_tessellate(tileExpr, lit(resolution), mode)
    // BNG resolution accepts an Int index (±1..±6) or a String key ("1km", "100m", ...).
    def rst_bng_tessellate(tileExpr: Column, resolution: Int): Column = rst_bng_tessellate(tileExpr, lit(resolution))
    def rst_bng_tessellate(tileExpr: Column, resolution: Int, mode: String): Column =
        rst_bng_tessellate(tileExpr, lit(resolution), mode)
    def rst_bng_tessellate(tileExpr: Column, resolution: String): Column = rst_bng_tessellate(tileExpr, lit(resolution))
    def rst_bng_tessellate(tileExpr: Column, resolution: String, mode: String): Column =
        rst_bng_tessellate(tileExpr, lit(resolution), mode)
    def rst_maketiles(tileExpr: Column, tileWidth: Int, tileHeight: Int): Column =
        rst_maketiles(tileExpr, lit(tileWidth), lit(tileHeight))
    def rst_retile(tileExpr: Column, tileWidth: Int, tileHeight: Int): Column =
        rst_retile(tileExpr, lit(tileWidth), lit(tileHeight))
    def rst_tooverlappingtiles(tileExpr: Column, tileWidth: Int, tileHeight: Int, overlap: Int): Column =
        rst_tooverlappingtiles(tileExpr, lit(tileWidth), lit(tileHeight), lit(overlap))
    def rst_h3_rastertogridavg(tileExpr: Column, resolution: Int): Column = rst_h3_rastertogridavg(tileExpr, lit(resolution))
    def rst_h3_rastertogridcount(tileExpr: Column, resolution: Int): Column = rst_h3_rastertogridcount(tileExpr, lit(resolution))
    def rst_h3_rastertogridmax(tileExpr: Column, resolution: Int): Column = rst_h3_rastertogridmax(tileExpr, lit(resolution))
    def rst_h3_rastertogridmin(tileExpr: Column, resolution: Int): Column = rst_h3_rastertogridmin(tileExpr, lit(resolution))
    def rst_h3_rastertogridmedian(tileExpr: Column, resolution: Int): Column = rst_h3_rastertogridmedian(tileExpr, lit(resolution))
    def rst_h3_rastertogridsum(tileExpr: Column, resolution: Int): Column = rst_h3_rastertogridsum(tileExpr, lit(resolution))
    def rst_h3_rastertogridvariance(tileExpr: Column, resolution: Int): Column = rst_h3_rastertogridvariance(tileExpr, lit(resolution))
    def rst_h3_rastertogridstddev(tileExpr: Column, resolution: Int): Column = rst_h3_rastertogridstddev(tileExpr, lit(resolution))
    def rst_quadbin_rastertogridavg(tileExpr: Column, resolution: Int): Column = rst_quadbin_rastertogridavg(tileExpr, lit(resolution))
    def rst_quadbin_rastertogridcount(tileExpr: Column, resolution: Int): Column = rst_quadbin_rastertogridcount(tileExpr, lit(resolution))
    def rst_quadbin_rastertogridmax(tileExpr: Column, resolution: Int): Column = rst_quadbin_rastertogridmax(tileExpr, lit(resolution))
    def rst_quadbin_rastertogridmin(tileExpr: Column, resolution: Int): Column = rst_quadbin_rastertogridmin(tileExpr, lit(resolution))
    def rst_quadbin_rastertogridmedian(tileExpr: Column, resolution: Int): Column = rst_quadbin_rastertogridmedian(tileExpr, lit(resolution))
    def rst_quadbin_rastertogridsum(tileExpr: Column, resolution: Int): Column = rst_quadbin_rastertogridsum(tileExpr, lit(resolution))
    def rst_quadbin_rastertogridvariance(tileExpr: Column, resolution: Int): Column = rst_quadbin_rastertogridvariance(tileExpr, lit(resolution))
    def rst_quadbin_rastertogridstddev(tileExpr: Column, resolution: Int): Column = rst_quadbin_rastertogridstddev(tileExpr, lit(resolution))
    // BNG reducer resolution accepts an Int index (±1..±6) or a String key ("1km", "100m", ...).
    def rst_bng_rastertogridavg(tileExpr: Column, resolution: Int): Column = rst_bng_rastertogridavg(tileExpr, lit(resolution))
    def rst_bng_rastertogridcount(tileExpr: Column, resolution: Int): Column = rst_bng_rastertogridcount(tileExpr, lit(resolution))
    def rst_bng_rastertogridmax(tileExpr: Column, resolution: Int): Column = rst_bng_rastertogridmax(tileExpr, lit(resolution))
    def rst_bng_rastertogridmin(tileExpr: Column, resolution: Int): Column = rst_bng_rastertogridmin(tileExpr, lit(resolution))
    def rst_bng_rastertogridmedian(tileExpr: Column, resolution: Int): Column = rst_bng_rastertogridmedian(tileExpr, lit(resolution))
    def rst_bng_rastertogridsum(tileExpr: Column, resolution: Int): Column = rst_bng_rastertogridsum(tileExpr, lit(resolution))
    def rst_bng_rastertogridvariance(tileExpr: Column, resolution: Int): Column = rst_bng_rastertogridvariance(tileExpr, lit(resolution))
    def rst_bng_rastertogridstddev(tileExpr: Column, resolution: Int): Column = rst_bng_rastertogridstddev(tileExpr, lit(resolution))
    def rst_bng_rastertogridavg(tileExpr: Column, resolution: String): Column = rst_bng_rastertogridavg(tileExpr, lit(resolution))
    def rst_bng_rastertogridcount(tileExpr: Column, resolution: String): Column = rst_bng_rastertogridcount(tileExpr, lit(resolution))
    def rst_bng_rastertogridmax(tileExpr: Column, resolution: String): Column = rst_bng_rastertogridmax(tileExpr, lit(resolution))
    def rst_bng_rastertogridmin(tileExpr: Column, resolution: String): Column = rst_bng_rastertogridmin(tileExpr, lit(resolution))
    def rst_bng_rastertogridmedian(tileExpr: Column, resolution: String): Column = rst_bng_rastertogridmedian(tileExpr, lit(resolution))
    def rst_bng_rastertogridsum(tileExpr: Column, resolution: String): Column = rst_bng_rastertogridsum(tileExpr, lit(resolution))
    def rst_bng_rastertogridvariance(tileExpr: Column, resolution: String): Column = rst_bng_rastertogridvariance(tileExpr, lit(resolution))
    def rst_bng_rastertogridstddev(tileExpr: Column, resolution: String): Column = rst_bng_rastertogridstddev(tileExpr, lit(resolution))
    def rst_asformat(tileExpr: Column, newFormat: String): Column = rst_asformat(tileExpr, lit(newFormat))
    def rst_clip(tileExpr: Column, clip: Column, cutlineAllTouched: Boolean): Column =
        rst_clip(tileExpr, clip, lit(cutlineAllTouched))
    def rst_filter(tileExpr: Column, kernelSize: Int, operation: String): Column =
        rst_filter(tileExpr, lit(kernelSize), lit(operation))
    def rst_mapalgebra(tiles: Column, expression: String): Column = rst_mapalgebra(tiles, lit(expression))
    def rst_ndvi(tileExpr: Column, redBand: Int, nirBand: Int): Column = rst_ndvi(tileExpr, lit(redBand), lit(nirBand))
    def rst_rastertoworldcoord(tileExpr: Column, pixelX: Int, pixelY: Int): Column =
        rst_rastertoworldcoord(tileExpr, lit(pixelX), lit(pixelY))
    def rst_rastertoworldcoordx(tileExpr: Column, pixelX: Int, pixelY: Int): Column =
        rst_rastertoworldcoordx(tileExpr, lit(pixelX), lit(pixelY))
    def rst_rastertoworldcoordy(tileExpr: Column, pixelX: Int, pixelY: Int): Column =
        rst_rastertoworldcoordy(tileExpr, lit(pixelX), lit(pixelY))
    def rst_transform(tileExpr: Column, targetSrid: Int): Column = rst_transform(tileExpr, lit(targetSrid))
    def rst_updatetype(tileExpr: Column, newType: String): Column = rst_updatetype(tileExpr, lit(newType))
    def rst_worldtorastercoord(tileExpr: Column, worldX: Double, worldY: Double): Column =
        rst_worldtorastercoord(tileExpr, lit(worldX), lit(worldY))
    def rst_worldtorastercoordx(tileExpr: Column, worldX: Double, worldY: Double): Column =
        rst_worldtorastercoordx(tileExpr, lit(worldX), lit(worldY))
    def rst_worldtorastercoordy(tileExpr: Column, worldX: Double, worldY: Double): Column =
        rst_worldtorastercoordy(tileExpr, lit(worldX), lit(worldY))

    // Web-mercator tile output (Column form)
    def rst_to_webmercator(tileExpr: Column): Column =
        ColumnAdapter(RST_ToWebMercator.name, Seq(tileExpr, lit("bilinear")))
    def rst_to_webmercator(tileExpr: Column, resampling: Column): Column =
        ColumnAdapter(RST_ToWebMercator.name, Seq(tileExpr, resampling))
    def rst_to_webmercator(tileExpr: Column, resampling: String): Column =
        rst_to_webmercator(tileExpr, lit(resampling))

    def rst_tilexyz(tileExpr: Column, z: Column, x: Column, y: Column): Column =
        ColumnAdapter(RST_TileXYZ.name, Seq(tileExpr, z, x, y, lit("PNG"), lit(256), lit("bilinear")))
    def rst_tilexyz(
        tileExpr: Column, z: Column, x: Column, y: Column,
        format: Column, size: Column, resampling: Column
    ): Column =
        ColumnAdapter(RST_TileXYZ.name, Seq(tileExpr, z, x, y, format, size, resampling))
    def rst_tilexyz(tileExpr: Column, z: Int, x: Int, y: Int): Column =
        rst_tilexyz(tileExpr, lit(z), lit(x), lit(y))
    def rst_tilexyz(
        tileExpr: Column, z: Int, x: Int, y: Int,
        format: String, size: Int, resampling: String
    ): Column =
        rst_tilexyz(tileExpr, lit(z), lit(x), lit(y), lit(format), lit(size), lit(resampling))

    def rst_xyzpyramid(tileExpr: Column, minZ: Column, maxZ: Column): Column =
        ColumnAdapter(RST_XYZPyramid.name, Seq(tileExpr, minZ, maxZ, lit("PNG"), lit(256), lit("bilinear")))
    def rst_xyzpyramid(
        tileExpr: Column, minZ: Column, maxZ: Column,
        format: Column, size: Column, resampling: Column
    ): Column =
        ColumnAdapter(RST_XYZPyramid.name, Seq(tileExpr, minZ, maxZ, format, size, resampling))
    def rst_xyzpyramid(tileExpr: Column, minZ: Int, maxZ: Int): Column =
        rst_xyzpyramid(tileExpr, lit(minZ), lit(maxZ))
    def rst_xyzpyramid(
        tileExpr: Column, minZ: Int, maxZ: Int,
        format: String, size: Int, resampling: String
    ): Column =
        rst_xyzpyramid(tileExpr, lit(minZ), lit(maxZ), lit(format), lit(size), lit(resampling))

    // Vector<->raster bridge (Column form)
    def rst_rasterize(
        geomWkb: Column, value: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        widthPx: Column, heightPx: Column, srid: Column
    ): Column =
        ColumnAdapter(RST_Rasterize.name, Seq(geomWkb, value, xmin, ymin, xmax, ymax, widthPx, heightPx, srid))

    def rst_polygonize(tileExpr: Column): Column =
        ColumnAdapter(RST_Polygonize.name, Seq(tileExpr, lit(1), lit(4)))
    def rst_polygonize(tileExpr: Column, band: Column): Column =
        ColumnAdapter(RST_Polygonize.name, Seq(tileExpr, band, lit(4)))
    def rst_polygonize(tileExpr: Column, band: Column, connectedness: Column): Column =
        ColumnAdapter(RST_Polygonize.name, Seq(tileExpr, band, connectedness))

    // Terrain analysis (DEM processing) - Column form
    def rst_slope(tileExpr: Column): Column =
        ColumnAdapter(RST_Slope.name, Seq(tileExpr, lit("degrees"), lit(Double.NaN)))
    def rst_slope(tileExpr: Column, unit: Column): Column =
        ColumnAdapter(RST_Slope.name, Seq(tileExpr, unit, lit(Double.NaN)))
    def rst_slope(tileExpr: Column, unit: Column, scale: Column): Column =
        ColumnAdapter(RST_Slope.name, Seq(tileExpr, unit, scale))
    def rst_slope(tileExpr: Column, unit: String): Column = rst_slope(tileExpr, lit(unit))
    def rst_slope(tileExpr: Column, unit: String, scale: Double): Column =
        rst_slope(tileExpr, lit(unit), lit(scale))

    def rst_aspect(tileExpr: Column): Column =
        ColumnAdapter(RST_Aspect.name, Seq(tileExpr, lit(false), lit(false)))
    def rst_aspect(tileExpr: Column, trigonometric: Column): Column =
        ColumnAdapter(RST_Aspect.name, Seq(tileExpr, trigonometric, lit(false)))
    def rst_aspect(tileExpr: Column, trigonometric: Column, zeroForFlat: Column): Column =
        ColumnAdapter(RST_Aspect.name, Seq(tileExpr, trigonometric, zeroForFlat))
    def rst_aspect(tileExpr: Column, trigonometric: Boolean): Column =
        rst_aspect(tileExpr, lit(trigonometric))
    def rst_aspect(tileExpr: Column, trigonometric: Boolean, zeroForFlat: Boolean): Column =
        rst_aspect(tileExpr, lit(trigonometric), lit(zeroForFlat))

    def rst_hillshade(tileExpr: Column): Column =
        ColumnAdapter(RST_Hillshade.name, Seq(tileExpr, lit(315.0), lit(45.0), lit(1.0)))
    def rst_hillshade(tileExpr: Column, azimuth: Column, altitude: Column, zFactor: Column): Column =
        ColumnAdapter(RST_Hillshade.name, Seq(tileExpr, azimuth, altitude, zFactor))
    def rst_hillshade(tileExpr: Column, azimuth: Double, altitude: Double): Column =
        rst_hillshade(tileExpr, lit(azimuth), lit(altitude), lit(1.0))
    def rst_hillshade(tileExpr: Column, azimuth: Double, altitude: Double, zFactor: Double): Column =
        rst_hillshade(tileExpr, lit(azimuth), lit(altitude), lit(zFactor))

    def rst_tri(tileExpr: Column): Column = ColumnAdapter(RST_TRI.name, Seq(tileExpr))
    def rst_tpi(tileExpr: Column): Column = ColumnAdapter(RST_TPI.name, Seq(tileExpr))
    def rst_roughness(tileExpr: Column): Column = ColumnAdapter(RST_Roughness.name, Seq(tileExpr))

    def rst_color_relief(tileExpr: Column, colorTablePath: Column): Column =
        ColumnAdapter(RST_ColorRelief.name, Seq(tileExpr, colorTablePath))
    def rst_color_relief(tileExpr: Column, colorTablePath: String): Column =
        rst_color_relief(tileExpr, lit(colorTablePath))

    // Spectral indices (Wave 8b) - all delegate to RST_MapAlgebra under the hood.
    def rst_evi(
        tileExpr: Column, redIdx: Column, nirIdx: Column, blueIdx: Column
    ): Column =
        ColumnAdapter(RST_EVI.name, Seq(tileExpr, redIdx, nirIdx, blueIdx,
            lit(1.0), lit(6.0), lit(7.5), lit(2.5)))
    def rst_evi(
        tileExpr: Column, redIdx: Column, nirIdx: Column, blueIdx: Column,
        l: Column, c1: Column, c2: Column, g: Column
    ): Column =
        ColumnAdapter(RST_EVI.name, Seq(tileExpr, redIdx, nirIdx, blueIdx, l, c1, c2, g))
    def rst_evi(tileExpr: Column, redIdx: Int, nirIdx: Int, blueIdx: Int): Column =
        rst_evi(tileExpr, lit(redIdx), lit(nirIdx), lit(blueIdx))
    def rst_evi(
        tileExpr: Column, redIdx: Int, nirIdx: Int, blueIdx: Int,
        l: Double, c1: Double, c2: Double, g: Double
    ): Column =
        rst_evi(tileExpr, lit(redIdx), lit(nirIdx), lit(blueIdx), lit(l), lit(c1), lit(c2), lit(g))

    def rst_savi(tileExpr: Column, redIdx: Column, nirIdx: Column): Column =
        ColumnAdapter(RST_SAVI.name, Seq(tileExpr, redIdx, nirIdx, lit(0.5)))
    def rst_savi(tileExpr: Column, redIdx: Column, nirIdx: Column, l: Column): Column =
        ColumnAdapter(RST_SAVI.name, Seq(tileExpr, redIdx, nirIdx, l))
    def rst_savi(tileExpr: Column, redIdx: Int, nirIdx: Int): Column =
        rst_savi(tileExpr, lit(redIdx), lit(nirIdx))
    def rst_savi(tileExpr: Column, redIdx: Int, nirIdx: Int, l: Double): Column =
        rst_savi(tileExpr, lit(redIdx), lit(nirIdx), lit(l))

    def rst_ndwi(tileExpr: Column, greenIdx: Column, nirIdx: Column): Column =
        ColumnAdapter(RST_NDWI.name, Seq(tileExpr, greenIdx, nirIdx))
    def rst_ndwi(tileExpr: Column, greenIdx: Int, nirIdx: Int): Column =
        rst_ndwi(tileExpr, lit(greenIdx), lit(nirIdx))

    def rst_nbr(tileExpr: Column, nirIdx: Column, swirIdx: Column): Column =
        ColumnAdapter(RST_NBR.name, Seq(tileExpr, nirIdx, swirIdx))
    def rst_nbr(tileExpr: Column, nirIdx: Int, swirIdx: Int): Column =
        rst_nbr(tileExpr, lit(nirIdx), lit(swirIdx))

    def rst_index(tileExpr: Column, formulaName: Column, bandMap: Column): Column =
        ColumnAdapter(RST_Index.name, Seq(tileExpr, formulaName, bandMap))
    def rst_index(tileExpr: Column, formulaName: String, bandMap: Column): Column =
        rst_index(tileExpr, lit(formulaName), bandMap)

    // Resample family - gdal.Warp -tr / -ts wrappers
    def rst_resample(tileExpr: Column, factor: Column): Column =
        ColumnAdapter(RST_Resample.name, Seq(tileExpr, factor, lit("bilinear")))
    def rst_resample(tileExpr: Column, factor: Column, algorithm: Column): Column =
        ColumnAdapter(RST_Resample.name, Seq(tileExpr, factor, algorithm))
    def rst_resample(tileExpr: Column, factor: Double): Column =
        rst_resample(tileExpr, lit(factor))
    def rst_resample(tileExpr: Column, factor: Double, algorithm: String): Column =
        rst_resample(tileExpr, lit(factor), lit(algorithm))

    def rst_resample_to_size(tileExpr: Column, widthPx: Column, heightPx: Column): Column =
        ColumnAdapter(RST_ResampleToSize.name, Seq(tileExpr, widthPx, heightPx, lit("bilinear")))
    def rst_resample_to_size(tileExpr: Column, widthPx: Column, heightPx: Column, algorithm: Column): Column =
        ColumnAdapter(RST_ResampleToSize.name, Seq(tileExpr, widthPx, heightPx, algorithm))
    def rst_resample_to_size(tileExpr: Column, widthPx: Int, heightPx: Int): Column =
        rst_resample_to_size(tileExpr, lit(widthPx), lit(heightPx))
    def rst_resample_to_size(tileExpr: Column, widthPx: Int, heightPx: Int, algorithm: String): Column =
        rst_resample_to_size(tileExpr, lit(widthPx), lit(heightPx), lit(algorithm))

    def rst_resample_to_res(tileExpr: Column, xRes: Column, yRes: Column): Column =
        ColumnAdapter(RST_ResampleToRes.name, Seq(tileExpr, xRes, yRes, lit("bilinear")))
    def rst_resample_to_res(tileExpr: Column, xRes: Column, yRes: Column, algorithm: Column): Column =
        ColumnAdapter(RST_ResampleToRes.name, Seq(tileExpr, xRes, yRes, algorithm))
    def rst_resample_to_res(tileExpr: Column, xRes: Double, yRes: Double): Column =
        rst_resample_to_res(tileExpr, lit(xRes), lit(yRes))
    def rst_resample_to_res(tileExpr: Column, xRes: Double, yRes: Double, algorithm: String): Column =
        rst_resample_to_res(tileExpr, lit(xRes), lit(yRes), lit(algorithm))

    // IDW interpolation - non-aggregator (arrays in a single row)
    def rst_gridfrompoints(
        points: Column, values: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        widthPx: Column, heightPx: Column, srid: Column
    ): Column =
        ColumnAdapter(RST_GridFromPoints.name, Seq(
            points, values, xmin, ymin, xmax, ymax, widthPx, heightPx, srid,
            lit(RST_GridFromPoints.DefaultPower),
            lit(RST_GridFromPoints.DefaultMaxPoints)
        ))
    def rst_gridfrompoints(
        points: Column, values: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        widthPx: Column, heightPx: Column, srid: Column,
        power: Column, maxPts: Column
    ): Column =
        ColumnAdapter(RST_GridFromPoints.name, Seq(
            points, values, xmin, ymin, xmax, ymax, widthPx, heightPx, srid, power, maxPts
        ))

    // IDW interpolation - aggregator (one point/value per row)
    def rst_gridfrompoints_agg(
        point: Column, value: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        widthPx: Column, heightPx: Column, srid: Column
    ): Column =
        ColumnAdapter(RST_GridFromPointsAgg.name, Seq(
            point, value, xmin, ymin, xmax, ymax, widthPx, heightPx, srid,
            lit(RST_GridFromPoints.DefaultPower),
            lit(RST_GridFromPoints.DefaultMaxPoints)
        ))
    def rst_gridfrompoints_agg(
        point: Column, value: Column,
        xmin: Column, ymin: Column, xmax: Column, ymax: Column,
        widthPx: Column, heightPx: Column, srid: Column,
        power: Column, maxPts: Column
    ): Column =
        ColumnAdapter(RST_GridFromPointsAgg.name, Seq(
            point, value, xmin, ymin, xmax, ymax, widthPx, heightPx, srid, power, maxPts
        ))

    // Pixel ops + extraction — Column form + scalar overloads
    def rst_fillnodata(tileExpr: Column): Column =
        ColumnAdapter(RST_FillNodata.name, Seq(tileExpr, lit(100.0), lit(0)))
    def rst_fillnodata(tileExpr: Column, maxSearchDist: Column): Column =
        ColumnAdapter(RST_FillNodata.name, Seq(tileExpr, maxSearchDist, lit(0)))
    def rst_fillnodata(tileExpr: Column, maxSearchDist: Column, smoothingIter: Column): Column =
        ColumnAdapter(RST_FillNodata.name, Seq(tileExpr, maxSearchDist, smoothingIter))
    def rst_fillnodata(tileExpr: Column, maxSearchDist: Double): Column =
        rst_fillnodata(tileExpr, lit(maxSearchDist))
    def rst_fillnodata(tileExpr: Column, maxSearchDist: Double, smoothingIter: Int): Column =
        rst_fillnodata(tileExpr, lit(maxSearchDist), lit(smoothingIter))

    def rst_sample(tileExpr: Column, geom: Column): Column =
        ColumnAdapter(RST_Sample.name, Seq(tileExpr, geom))

    def rst_setsrid(tileExpr: Column, srid: Column): Column =
        ColumnAdapter(RST_SetSrid.name, Seq(tileExpr, srid))
    def rst_setsrid(tileExpr: Column, srid: Int): Column =
        rst_setsrid(tileExpr, lit(srid))

    def rst_histogram(tileExpr: Column): Column =
        ColumnAdapter(RST_Histogram.name, Seq(
            tileExpr, lit(256), lit(null).cast("double"), lit(null).cast("double"), lit(false)
        ))
    def rst_histogram(tileExpr: Column, nBuckets: Column): Column =
        ColumnAdapter(RST_Histogram.name, Seq(
            tileExpr, nBuckets, lit(null).cast("double"), lit(null).cast("double"), lit(false)
        ))
    def rst_histogram(tileExpr: Column, nBuckets: Column, minVal: Column, maxVal: Column): Column =
        ColumnAdapter(RST_Histogram.name, Seq(
            tileExpr, nBuckets, minVal, maxVal, lit(false)
        ))
    def rst_histogram(
        tileExpr: Column, nBuckets: Column, minVal: Column, maxVal: Column, includeNodata: Column
    ): Column =
        ColumnAdapter(RST_Histogram.name, Seq(
            tileExpr, nBuckets, minVal, maxVal, includeNodata
        ))
    def rst_histogram(tileExpr: Column, nBuckets: Int): Column =
        rst_histogram(tileExpr, lit(nBuckets))

    def rst_threshold(tileExpr: Column, op: Column, value: Column): Column =
        ColumnAdapter(RST_Threshold.name, Seq(tileExpr, op, value))
    def rst_threshold(tileExpr: Column, op: String, value: Double): Column =
        rst_threshold(tileExpr, lit(op), lit(value))

    def rst_buildoverviews(tileExpr: Column, levels: Column): Column =
        ColumnAdapter(RST_BuildOverviews.name, Seq(tileExpr, levels, lit("average")))
    def rst_buildoverviews(tileExpr: Column, levels: Column, resampling: Column): Column =
        ColumnAdapter(RST_BuildOverviews.name, Seq(tileExpr, levels, resampling))
    def rst_buildoverviews(tileExpr: Column, levels: Array[Int]): Column =
        rst_buildoverviews(tileExpr, lit(levels))
    def rst_buildoverviews(tileExpr: Column, levels: Array[Int], resampling: String): Column =
        rst_buildoverviews(tileExpr, lit(levels), lit(resampling))

    def rst_band(tileExpr: Column, bandIndex: Column): Column =
        ColumnAdapter(RST_Band.name, Seq(tileExpr, bandIndex))
    def rst_band(tileExpr: Column, bandIndex: Int): Column =
        rst_band(tileExpr, lit(bandIndex))

    // Analysis (COG / proximity / contour / viewshed) — Column form + scalar overloads
    def rst_cog_convert(tileExpr: Column): Column =
        ColumnAdapter(RST_CogConvert.name, Seq(tileExpr, lit("DEFLATE"), lit(512), lit("AVERAGE")))
    def rst_cog_convert(tileExpr: Column, compression: Column): Column =
        ColumnAdapter(RST_CogConvert.name, Seq(tileExpr, compression, lit(512), lit("AVERAGE")))
    def rst_cog_convert(tileExpr: Column, compression: Column, blocksize: Column): Column =
        ColumnAdapter(RST_CogConvert.name, Seq(tileExpr, compression, blocksize, lit("AVERAGE")))
    def rst_cog_convert(
        tileExpr: Column, compression: Column, blocksize: Column, overviewResampling: Column
    ): Column = ColumnAdapter(RST_CogConvert.name, Seq(tileExpr, compression, blocksize, overviewResampling))
    def rst_cog_convert(tileExpr: Column, compression: String): Column =
        rst_cog_convert(tileExpr, lit(compression))
    def rst_cog_convert(tileExpr: Column, compression: String, blocksize: Int): Column =
        rst_cog_convert(tileExpr, lit(compression), lit(blocksize))
    def rst_cog_convert(
        tileExpr: Column, compression: String, blocksize: Int, overviewResampling: String
    ): Column = rst_cog_convert(tileExpr, lit(compression), lit(blocksize), lit(overviewResampling))

    def rst_proximity(tileExpr: Column): Column =
        ColumnAdapter(RST_Proximity.name, Seq(
            tileExpr, lit(null).cast("string"), lit("GEO"), lit(null).cast("double")
        ))
    def rst_proximity(tileExpr: Column, targetValues: Column): Column =
        ColumnAdapter(RST_Proximity.name, Seq(
            tileExpr, targetValues, lit("GEO"), lit(null).cast("double")
        ))
    def rst_proximity(tileExpr: Column, targetValues: Column, distUnits: Column): Column =
        ColumnAdapter(RST_Proximity.name, Seq(
            tileExpr, targetValues, distUnits, lit(null).cast("double")
        ))
    def rst_proximity(
        tileExpr: Column, targetValues: Column, distUnits: Column, maxDistance: Column
    ): Column = ColumnAdapter(RST_Proximity.name, Seq(tileExpr, targetValues, distUnits, maxDistance))

    def rst_contour(tileExpr: Column, levels: Column): Column =
        ColumnAdapter(RST_Contour.name, Seq(tileExpr, levels, lit(0.0), lit(0.0), lit("elev")))
    def rst_contour(tileExpr: Column, levels: Column, interval: Column): Column =
        ColumnAdapter(RST_Contour.name, Seq(tileExpr, levels, interval, lit(0.0), lit("elev")))
    def rst_contour(
        tileExpr: Column, levels: Column, interval: Column, base: Column
    ): Column = ColumnAdapter(RST_Contour.name, Seq(tileExpr, levels, interval, base, lit("elev")))
    def rst_contour(
        tileExpr: Column, levels: Column, interval: Column, base: Column, attrField: Column
    ): Column = ColumnAdapter(RST_Contour.name, Seq(tileExpr, levels, interval, base, attrField))

    def rst_viewshed(tileExpr: Column, observerGeom: Column, observerHeight: Column): Column =
        ColumnAdapter(RST_Viewshed.name, Seq(
            tileExpr, observerGeom, observerHeight, lit(1.6), lit(null).cast("double")
        ))
    def rst_viewshed(
        tileExpr: Column, observerGeom: Column, observerHeight: Column, targetHeight: Column
    ): Column = ColumnAdapter(RST_Viewshed.name, Seq(
        tileExpr, observerGeom, observerHeight, targetHeight, lit(null).cast("double")
    ))
    def rst_viewshed(
        tileExpr: Column, observerGeom: Column, observerHeight: Column,
        targetHeight: Column, maxDistance: Column
    ): Column = ColumnAdapter(RST_Viewshed.name, Seq(
        tileExpr, observerGeom, observerHeight, targetHeight, maxDistance
    ))

}
