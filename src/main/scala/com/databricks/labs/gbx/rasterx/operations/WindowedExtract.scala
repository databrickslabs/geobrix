package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager}
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
  * Extracts an axis-aligned pixel window from a raster directly, without shelling out to
  * `gdal.Translate -srcwin`. This is the fast path under [[ReTile.getTile]] (shared by
  * `rst_tooverlappingtiles`, `rst_retile`, and `rst_maketiles`).
  *
  * The downstream serializer ([[com.databricks.labs.gbx.rasterx.gdal.RasterDriver.writeToBytes]])
  * re-encodes the returned Dataset with its own creation options, so compression/byte-layout
  * parity with `gdal.Translate` is moot. What MUST match: pixels, geotransform, SRS, NoData,
  * band structure, and the per-band/per-dataset attributes copied below.
  *
  * Correctness over speed: the fast path runs only when [[simpleEnough]] holds. Anything it
  * cannot faithfully reproduce (mixed dtype, real mask bands, GCPs, RPC/GEOLOCATION
  * georeferencing) FALLS BACK to the proven [[GDALTranslate.executeTranslate]] with the exact
  * same `gdal_translate -srcwin ...` command. Worst case is "not faster," never "wrong."
  */
private[rasterx] object WindowedExtract {

    /**
      * Extract the axis-aligned window `(xStart, yStart, xOffset, yOffset)` from `ds`.
      *
      * Returns `(Dataset, metadata)` with the SAME metadata-map keys/shape as
      * [[GDALTranslate.executeTranslate]] so downstream `writeToBytes` behaves identically.
      * Caller must release the returned Dataset (and unlink the `"path"` /vsimem file).
      */
    def extract(
        ds: Dataset,
        options: Map[String, String],
        xStart: Int,
        yStart: Int,
        xOffset: Int,
        yOffset: Int
    ): (Dataset, Map[String, String]) = {
        // Opt-in CF scale/offset decode (only NetCDF_Reader sets applyScale=true). When a band
        // carries non-identity scale_factor/add_offset, decode to physical Float64 via the proven
        // gdal_translate -unscale path, matching the light netcdf_gbx reader (xarray
        // mask_and_scale=True). Default OFF => every other raster reader is byte-for-byte unchanged.
        val applyScale = options.getOrElse("applyScale", "false").toBoolean
        if (applyScale && hasNonIdentityScale(ds)) {
            return fallback(ds, options + ("unscale" -> "true"), xStart, yStart, xOffset, yOffset)
        }

        if (!simpleEnough(ds)) {
            return fallback(ds, options, xStart, yStart, xOffset, yOffset)
        }

        val bandCount = ds.getRasterCount
        val dtype = ds.GetRasterBand(1).getDataType
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val rasterPath = s"/vsimem/retile_$uuid.tif"
        val drv = GDALManager.gtiffDriver()
        val out = drv.Create(rasterPath, xOffset, yOffset, bandCount, dtype)

        // Dataset-level: window-shifted geotransform (full formula incl. rotation terms),
        // projection, and default-domain metadata.
        val gt = new Array[Double](6)
        ds.GetGeoTransform(gt)
        out.SetGeoTransform(
          Array(
            gt(0) + xStart * gt(1) + yStart * gt(2),
            gt(1),
            gt(2),
            gt(3) + xStart * gt(4) + yStart * gt(5),
            gt(4),
            gt(5)
          )
        )
        out.SetProjection(ds.GetProjection())
        Option(ds.GetMetadata_Dict()).foreach(md => if (!md.isEmpty) out.SetMetadata(md))

        val pixBytes = gdal.GetDataTypeSize(dtype) / 8
        val buf = new Array[Byte](xOffset * yOffset * pixBytes)
        var b = 1
        while (b <= bandCount) {
            val sb = ds.GetRasterBand(b)
            val db = out.GetRasterBand(b)
            // Raw bytes at the band's native dtype: no resample, no type-convert.
            sb.ReadRaster(xStart, yStart, xOffset, yOffset, xOffset, yOffset, dtype, buf)
            db.WriteRaster(0, 0, xOffset, yOffset, xOffset, yOffset, dtype, buf)

            val nd = new Array[java.lang.Double](1)
            sb.GetNoDataValue(nd)
            if (nd(0) != null) db.SetNoDataValue(nd(0).doubleValue()) // handles NaN-nodata too

            db.SetColorInterpretation(sb.GetColorInterpretation())

            val ct = sb.GetColorTable()
            if (ct != null) db.SetColorTable(ct)

            val scale = new Array[java.lang.Double](1)
            sb.GetScale(scale)
            if (scale(0) != null) db.SetScale(scale(0).doubleValue())

            val offset = new Array[java.lang.Double](1)
            sb.GetOffset(offset)
            if (offset(0) != null) db.SetOffset(offset(0).doubleValue())

            val unit = sb.GetUnitType()
            if (unit != null && unit.nonEmpty) db.SetUnitType(unit)

            Option(sb.GetMetadata_Dict()).foreach(md => if (!md.isEmpty) db.SetMetadata(md))
            b += 1
        }
        out.FlushCache()

        val sourcePath = Option(ds.GetFileList())
            .flatMap(_.asScala.headOption.map(_.toString))
            .getOrElse("unknown source path")
        val meta = Map(
          "path" -> rasterPath,
          "sourcePath" -> sourcePath,
          "driver" -> "GTiff",
          "format" -> "GTiff",
          "last_command" -> s"windowed_extract -srcwin $xStart $yStart $xOffset $yOffset",
          "last_error" -> "",
          "all_parents" -> s"$sourcePath;${options.getOrElse("all_parents", "")}",
          "size" -> "-1",
          "compression" -> options.getOrElse("compression", "DEFLATE"),
          "isZipped" -> "false",
          "isSubset" -> "false"
        )
        (out, meta)
    }

    /**
      * True when the fast path can faithfully reproduce a `gdal.Translate -srcwin` of `ds`.
      *
      * Requires ALL of: uniform per-band data type ([[org.gdal.gdal.Driver.Create]] takes one
      * dtype); no real mask bands (every band's `GetMaskFlags()` is only `GMF_ALL_VALID` or
      * `GMF_NODATA` — same predicate as [[BandAccessors.isEmpty]] treats as "no separate mask");
      * no GCPs; no `RPC`/`GEOLOCATION` metadata domain. Anything else => fall back.
      */
    private def simpleEnough(ds: Dataset): Boolean = {
        val bandCount = ds.getRasterCount
        if (bandCount <= 0) return false
        val dtype0 = ds.GetRasterBand(1).getDataType

        var b = 1
        while (b <= bandCount) {
            val band = ds.GetRasterBand(b)
            if (band.getDataType != dtype0) return false
            // A real (per-dataset/alpha/separate) mask band is anything beyond all-valid or
            // the nodata-derived mask, which Create+WriteRaster + SetNoDataValue reproduces.
            val flags = band.GetMaskFlags()
            val onlyImplicit =
                (flags & gdalconstConstants.GMF_ALL_VALID) != 0 ||
                    (flags & gdalconstConstants.GMF_NODATA) != 0
            if (!onlyImplicit) return false
            b += 1
        }

        if (ds.GetGCPCount() != 0) return false

        val domains = Option(ds.GetMetadataDomainList())
            .map(_.asScala.map(_.toString).toSet)
            .getOrElse(Set.empty[String])
        if (domains.contains("RPC") || domains.contains("GEOLOCATION")) return false

        true
    }

    /**
      * Exact-semantics fallback to `gdal.Translate -srcwin` for non-simple datasets.
      *
      * Identical to the original [[ReTile.getTile]] behaviour: the output path takes the SOURCE
      * driver's extension so the format round-trips faithfully. A GTiff target would be wrong
      * here — the very cases that reach the fallback (e.g. a mixed-dtype VRT) cannot be written
      * to GTiff at all ("different datatypes per different bands"), so the source format must be
      * preserved.
      */
    private def fallback(
        ds: Dataset,
        options: Map[String, String],
        xStart: Int,
        yStart: Int,
        xOffset: Int,
        yOffset: Int
    ): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val extension = GDAL.getExtension(ds.GetDriver.getShortName)
        val rasterPath = s"/vsimem/retile_$uuid.$extension"
        // When the caller opts in (NetCDF applyScale), append -unscale -ot Float64 so GDAL decodes
        // packed values to physical Float64 (raw*scale+offset) with its own per-dtype/nodata
        // handling; -unscale also drops the scale/offset tags, so they are NOT re-applied on any
        // downstream re-read.
        // When unscaling, also pass -a_nodata nan so GDAL writes actual IEEE NaN at every pixel whose
        // raw value equals the source _FillValue / nodata. Without this, gdal.Translate keeps the raw
        // packed integer (e.g. -32768) as the float64 output value even though it has already set the
        // output nodata declaration to that sentinel -- the pixel is "nodata" in metadata but not NaN
        // in the byte buffer. Light (xarray mask_and_scale=True) returns NaN for fill cells, so heavy
        // must also materialise NaN for cross-tier assert_allclose(equal_nan=True) to pass.
        // -a_nodata nan is safe on files with no fill cells: the nodata declaration just moves to NaN
        // but no pixel value changes (integer inputs never decode to NaN via raw*scale+offset).
        val unscale = if (options.getOrElse("unscale", "false").toBoolean) " -unscale -ot Float64 -a_nodata nan" else ""
        GDALTranslate.executeTranslate(
          rasterPath,
          ds,
          command = s"gdal_translate -srcwin $xStart $yStart $xOffset $yOffset$unscale",
          options
        )
    }

    /**
      * True when any band carries a non-identity CF scale_factor/add_offset (`GetScale != 1.0` or
      * `GetOffset != 0.0`). Used to gate the opt-in applyScale unscale path — an all-identity raster
      * needs no decode and stays on the raw fast path.
      */
    private def hasNonIdentityScale(ds: Dataset): Boolean = {
        val n = ds.getRasterCount
        (1 to n).exists { b =>
            val sb = ds.GetRasterBand(b)
            val s = new Array[java.lang.Double](1); sb.GetScale(s)
            val o = new Array[java.lang.Double](1); sb.GetOffset(o)
            (s(0) != null && s(0).doubleValue() != 1.0) || (o(0) != null && o(0).doubleValue() != 0.0)
        }
    }

}
