package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.OperatorOptions
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.ogr.ogr.{CreateGeometryFromWkb, GetDriverByName}
import org.gdal.ogr.ogrConstants.{OFTReal, wkbPoint, wkbPolygon}
import org.gdal.ogr.{DataSource, Feature, FieldDefn, ogr}
import org.locationtech.jts.geom.{Geometry, Point, Polygon}

import java.nio.file.{Files, Paths}
import java.util.{Vector => JVector}
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Burns geometries and values into a raster via OGR in-memory layer and gdal.RasterizeLayer. */
object GDALRasterize {

    private val layerName = "FEATURES"
    private val valueFieldName = "VALUES"

    /** Burns geoms (and values or Z) into a new raster at origin with given pixel dimensions; returns (Dataset, metadata). Caller must release. */
    def executeRasterize(
        geoms: Seq[Geometry],
        values: Option[Seq[Double]],
        origin: Point,
        xWidth: Int,
        yWidth: Int,
        xSize: Double,
        ySize: Double,
        noDataValue: Double,
        options: Map[String, String]
    ): (Dataset, Map[String, String]) = {
        val format = options.getOrElse("format", "GTiff")
        val driver = gdal.GetDriverByName(format)
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val outShortName = driver.getShortName
        val extension = GDAL.getExtension(outShortName)
        val outputPath = s"/vsimem/clip_to_geom_$uuid.$extension"

        val createOptionsVec = new JVector[String]()
        createOptionsVec.addAll(Seq("COMPRESS=ZSTD", "TILED=YES").asJava)

        val newRaster = driver.Create(outputPath, xWidth, yWidth, 1, gdalconstConstants.GDT_Float64, createOptionsVec)
        val rasterCRS = if (geoms.isEmpty) SpatialRefOps.fromEPSGCode(origin.getSRID) else SpatialRefOps.fromEPSGCode(geoms.head.getSRID)
        newRaster.SetSpatialRef(rasterCRS)
        newRaster.SetGeoTransform(Array(origin.getX, xSize, 0.0, origin.getY, 0.0, ySize))

        val outputBand = newRaster.GetRasterBand(1)
        outputBand.SetNoDataValue(noDataValue)
        outputBand.FlushCache()

        newRaster.FlushCache()

        if (geoms.isEmpty) {

            val errorMsg = "No geometries to rasterize."
            newRaster.delete()
            val newOptions = Map(
              "path" -> outputPath,
              "parentPath" -> "",
              "driver" -> format,
              "last_command" -> "",
              "last_error" -> errorMsg,
              "all_parents" -> ""
            )
            return (newRaster, newOptions)
        }

        val valuesToBurn = values.getOrElse(geoms.map(g => JTS.anyPoint(g).getCoordinate.z)) // can come back and make this the mean
        val vecDataSource = writeToDataSource(geoms, valuesToBurn)

        val command = s"gdal_rasterize ATTRIBUTE=$valueFieldName"
        val effectiveCommand = OperatorOptions.appendOptions(command, options, newRaster)
        val bands = Array(1)
        val burnValues = Array(0.0)
        val rasterizeOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
        gdal.RasterizeLayer(newRaster, bands, vecDataSource.GetLayer(0), burnValues, rasterizeOptionsVec)
        outputBand.FlushCache()

        newRaster.FlushCache()
        newRaster.delete()
        val size = Try(Files.size(Paths.get(outputPath))).getOrElse(-1)
        val errorMsg = gdal.GetLastErrorMsg
        val newOptions = Map(
          "path" -> outputPath,
          "parentPath" -> "",
          "driver" -> format,
          "last_command" -> effectiveCommand,
          "last_error" -> errorMsg,
          "size" -> size.toString,
          "all_parents" -> ""
        )
        (newRaster, newOptions)
    }

    /** Creates an OGR in-memory (or file) DataSource with a layer of geoms and value field; caller must dispose. */
    private def writeToDataSource(
        geoms: Seq[Geometry],
        valuesToBurn: Seq[Double],
        format: String = "Memory",
        path: String = "mem"
    ): DataSource = {
        ogr.RegisterAll()

        val vecDriver = GetDriverByName(format)
        val vecDataSource = vecDriver.CreateDataSource(path)

        val ogrGeometryType = geoms.head match {
            case _: Point   => wkbPoint
            case _: Polygon => wkbPolygon
            case _       => throw new UnsupportedOperationException("Only Point and Polygon geometries are supported for rasterization.")
        }

        val layer = vecDataSource.CreateLayer(layerName, SpatialRefOps.fromEPSGCode(geoms.head.getSRID), ogrGeometryType)

        val attributeField = new FieldDefn(valueFieldName, OFTReal)
        layer.CreateField(attributeField)

        geoms
            .zip(valuesToBurn)
            .foreach({ case (g: Geometry, v: Double) =>
                val geom = CreateGeometryFromWkb(JTS.toWKB(g))
                val featureDefn = layer.GetLayerDefn()
                val feature = new Feature(featureDefn)
                feature.SetGeometry(geom)
                feature.SetField(valueFieldName, v)
                layer.CreateFeature(feature)
            })

        layer.SyncToDisk()
        layer.delete()
        vecDataSource
    }

}
