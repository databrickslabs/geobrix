package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.{GDALBuildVRT, GDALTranslate}
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.gdal.gdal.Dataset

import java.io.File
import java.nio.file.{Files, Paths}
import scala.xml.{Elem, UnprefixedAttribute, XML}

/** Combines multiple rasters with a Python pixel function (e.g. average) via VRT and gdal_translate. */
object PixelCombineRasters {

    /** Builds VRT, injects Python pixel function, translates to raster; returns (Dataset, metadata). Caller must release. */
    def combine(
        dss: Array[Dataset],
        options: Map[String, String],
        pythonFunc: String,
        pythonFuncName: String
    ): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val outShortName = dss.head.GetDriver().getShortName
        val extension = GDAL.getExtension(outShortName)
        val vrtPath = s"${NodeFilePathUtil.rootPath}/combine_rasters_vrt_$uuid.vrt"
        val rasterPath = s"/vsimem/combine_rasters_$uuid.$extension"

        val vrtRaster = GDALBuildVRT.executeVRT(
          vrtPath,
          dss,
          options,
          command = s"gdalbuildvrt -resolution highest"
        )
        vrtRaster._1.delete()
        val vrtRefreshed = RasterDriver.read(vrtPath, vrtRaster._2)

        addPixelFunction(vrtPath, pythonFunc, pythonFuncName)

        val result = GDALTranslate.executeTranslate(
          rasterPath,
          vrtRefreshed,
          command = s"gdal_translate",
          options
        )

        Files.deleteIfExists(Paths.get(vrtPath))

        result
    }

    /** Injects PixelFunctionType, PixelFunctionLanguage=Python, and PixelFunctionCode into the VRT XML at vrtPath. */
    def addPixelFunction(vrtPath: String, pixFuncCode: String, pixFuncName: String): Unit = {
        val pixFuncTypeEl = <PixelFunctionType>{pixFuncName}</PixelFunctionType>
        val pixFuncLangEl = <PixelFunctionLanguage>Python</PixelFunctionLanguage>
        val pixFuncCodeEl = <PixelFunctionCode>
            {scala.xml.Unparsed(s"<![CDATA[$pixFuncCode]]>")}
        </PixelFunctionCode>

        val vrtContent = XML.loadFile(new File(vrtPath))
        val vrtWithPixFunc = vrtContent match {
            case body @ Elem(_, _, _, _, child @ _*) => body.copy(
                  child = child.map {
                      case el @ Elem(_, "VRTRasterBand", _, _, child @ _*) => el
                              .asInstanceOf[Elem]
                              .copy(
                                child = Seq(pixFuncTypeEl, pixFuncLangEl, pixFuncCodeEl) ++ child,
                                attributes = el
                                    .asInstanceOf[Elem]
                                    .attributes
                                    .append(
                                      new UnprefixedAttribute("subClass", "VRTDerivedRasterBand", scala.xml.Null)
                                    )
                              )
                      case el                                              => el
                  }
                )
        }

        XML.save(vrtPath, vrtWithPixFunc)

    }

}
