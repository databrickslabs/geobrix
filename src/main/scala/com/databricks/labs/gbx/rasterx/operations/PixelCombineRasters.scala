package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.{GDALBuildVRT, GDALTranslate}
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.gdal.gdal.Dataset

import java.io.File
import java.nio.file.{Files, Paths}
import scala.xml.{Elem, UnprefixedAttribute, XML}

/** Combines multiple rasters with a Python pixel function (e.g. average) via VRT and gdal_translate. */
object PixelCombineRasters {

    /**
      * Builds VRT, injects Python pixel function, translates to raster; returns (Dataset, metadata). Caller must release.
      *
      * Every band of every input collapses into a single derived output band (rst_derivedband): the pixel
      * function receives all bands in `in_ar` and emits exactly one band.
      */
    def combine(
        dss: Array[Dataset],
        options: Map[String, String],
        pythonFunc: String,
        pythonFuncName: String
    ): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val outShortName = dss.head.GetDriver().getShortName
        val extension = GDAL.getExtension(outShortName)
        // Ensure the per-JVM staging dir exists. gdal.BuildVRT silently
        // produces an unwritten Dataset if the parent dir is missing, and
        // the subsequent RasterDriver.read(vrtPath) then throws
        // "No such file or directory". Only ClipToGeom previously created
        // this dir, leaving derivedband broken if it was the first op to
        // hit a fresh JVM.
        Files.createDirectories(NodeFilePathUtil.rootPath)
        val vrtPath = s"${NodeFilePathUtil.rootPath}/combine_rasters_vrt_$uuid.vrt"
        val rasterPath = s"/vsimem/combine_rasters_$uuid.$extension"

        // `-separate` stacks every band of every input dataset as a distinct
        // VRT band/source, in dataset-then-band order:
        //   [ds0-b1, ds0-b2, ..., ds0-bN, ds1-b1, ..., ds1-bN, ..., dsK-bN].
        // Without it, gdalbuildvrt would overlay inputs band-by-band (last wins),
        // which is not what derivedband wants. `-separate` makes every input
        // band an addressable VRT source; addPixelFunction then gathers ALL
        // sources into one derived band (1 output band).
        val vrtRaster = GDALBuildVRT.executeVRT(
          vrtPath,
          dss,
          options,
          command = s"gdalbuildvrt -resolution highest -separate"
        )
        vrtRaster._1.delete()

        // Inject the pixel function BEFORE re-opening the VRT. gdal.Open
        // parses VRT XML into an in-memory band structure at Open time, so
        // any mutation of the on-disk file performed after Open is invisible
        // to the Dataset handle passed to gdal.Translate — Translate then
        // runs a default multi-source mosaic (last-source-wins per pixel)
        // instead of evaluating the pixel function, silently returning one
        // of the inputs.
        addPixelFunction(vrtPath, pythonFunc, pythonFuncName)

        // GDAL evaluates <PixelFunctionLanguage>Python</...> during VRT read-back and translate.
        val result = GDALManager.withVrtPython {
            val vrtRefreshed = RasterDriver.read(vrtPath, vrtRaster._2)
            // Closed with delete(), not releaseDataset: a VRT's file list names its sources (the inputs'
            // files), which are not this method's to unlink or release.
            try {
                GDALTranslate.executeTranslate(
                  rasterPath,
                  vrtRefreshed,
                  command = s"gdal_translate",
                  options
                )
            } finally vrtRefreshed.delete()
        }

        Files.deleteIfExists(Paths.get(vrtPath))

        result
    }

    /**
      * Rewrites the `-separate` VRT's bands into one derived `VRTRasterBand` driven by
      * the supplied Python pixel function (see [[combine]]): the source elements
      * (`SimpleSource` / `ComplexSource` / `AveragedSource`) of EVERY band go into a
      * SINGLE derived band, so the pixel function receives all input bands in `in_ar`
      * and produces exactly one output band. Matches the documented/unit-tested
      * `rst_derivedband` contract (1 output band) whether the input is N single-band
      * tiles or one N-band tile.
      */
    def addPixelFunction(
        vrtPath: String,
        pixFuncCode: String,
        pixFuncName: String
    ): Unit = {
        def pixFuncTypeEl = <PixelFunctionType>{pixFuncName}</PixelFunctionType>
        def pixFuncLangEl = <PixelFunctionLanguage>Python</PixelFunctionLanguage>
        def pixFuncCodeEl = <PixelFunctionCode>
            {scala.xml.Unparsed(s"<![CDATA[$pixFuncCode]]>")}
        </PixelFunctionCode>

        val sourceLabels = Set("SimpleSource", "ComplexSource", "AveragedSource")

        // Builds one derived VRTRasterBand from a list of source elements, basing
        // attributes on `templateBand` and forcing band="<bandNo>".
        def derivedBand(templateBand: Elem, sources: Seq[scala.xml.Node], bandNo: Int): Elem =
            templateBand.copy(
              child = Seq(pixFuncTypeEl, pixFuncLangEl, pixFuncCodeEl) ++ sources,
              attributes = templateBand.attributes
                  .remove("band")
                  .remove("subClass")
                  .append(new UnprefixedAttribute("band", bandNo.toString, scala.xml.Null))
                  .append(new UnprefixedAttribute("subClass", "VRTDerivedRasterBand", scala.xml.Null))
            )

        val vrtContent = XML.loadFile(new File(vrtPath))
        val vrtWithPixFunc = vrtContent match {
            case body @ Elem(_, _, _, _, child @ _*) =>
                val rasterBands = child.collect {
                    case el @ Elem(_, "VRTRasterBand", _, _, _*) => el.asInstanceOf[Elem]
                }
                // Gather a band's source elements, STRIPPING any `<NODATA>` child that
                // `gdalbuildvrt` emits for inputs that declare a NoData value. That
                // element makes GDAL pre-mask the source's NoData pixels to the band
                // fill (0) BEFORE the Python pixel function runs, so the function would
                // see 0 instead of the raw sentinel and could not exclude those pixels.
                // The pixel function is the single
                // authority on NoData here (its baked-in NODATA literal), so it must
                // receive raw values, sentinels included.
                def stripNoData(source: scala.xml.Node): scala.xml.Node = source match {
                    case el: Elem =>
                        el.copy(child = el.child.filterNot {
                            case Elem(_, "NODATA", _, _, _*) => true
                            case _                           => false
                        })
                    case other => other
                }
                def sourcesOf(band: Elem): Seq[scala.xml.Node] =
                    band.child.collect {
                        case el @ Elem(_, label, _, _, _*) if sourceLabels.contains(label) =>
                            stripNoData(el)
                    }
                val baseBand = rasterBands.headOption.getOrElse(
                    <VRTRasterBand dataType="Float64" band="1"/>
                )

                // All sources across every band -> one derived band.
                val newBands: Seq[Elem] = Seq(derivedBand(baseBand, rasterBands.flatMap(sourcesOf), 1))

                // Drop all original VRTRasterBand children; splice the new band in at
                // the position of the first one (preserving SRS / GeoTransform / metadata).
                var inserted = false
                val newChild = child.flatMap {
                    case Elem(_, "VRTRasterBand", _, _, _*) =>
                        if (inserted) Seq.empty
                        else { inserted = true; newBands }
                    case other => Seq(other)
                }
                body.copy(child = newChild)
        }

        XML.save(vrtPath, vrtWithPixFunc)

    }

}
