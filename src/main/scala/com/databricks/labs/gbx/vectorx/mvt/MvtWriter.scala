package com.databricks.labs.gbx.vectorx.mvt

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.rasterx.tile.TileMath
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.gdal
import org.gdal.ogr.ogr.{CreateGeometryFromWkb, GetDriverByName}
import org.gdal.ogr.{Feature, FieldDefn}
import org.gdal.ogr.ogrConstants.{OFSTBoolean, OFTInteger, OFTInteger64, OFTReal, OFTString, wkbUnknown}
import org.gdal.osr.SpatialReference

import java.nio.file.{Files, Path, Paths}
import java.util.{Vector => JVector}
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * Helper that wraps GDAL's OGR MVT driver to encode a list of `(geom_wkb, attrs_map)` tuples
  * into a single Mapbox Vector Tile (MVT) protobuf blob.
  *
  * Caller passes geometries in **tile-local pixel coordinates `[0, extent]`** — the standard
  * MVT contract: x ∈ [0, extent] left→right, y ∈ [0, extent] top→bottom (origin upper-left,
  * y-down), matching the light `pyvx` tier (which feeds `[0,extent]` straight to
  * `mapbox_vector_tile.encode(..., y_coord_down=True)`).
  *
  * The OGR MVT *creation* driver does geographic tiling — it interprets layer geometry as
  * EPSG:3857 metres and bins into web-mercator tiles, so it will NOT honor raw `[0,extent]`
  * input (a tiny tile-local polygon collapses sub-pixel into a single z0 world tile and gets
  * dropped). To make the driver round-trip tile-local coords 1:1, we affine-map each input
  * coordinate from `[0, extent]` into the full web-mercator z0 world extent before handing it
  * to the driver; the driver's z0/EXTENT quantization then maps it straight back to the
  * original `[0,extent]` tile-local value (±1 from integer quantization). With `MINZOOM=0`,
  * `MAXZOOM=0`, `EXTENT=extent`, the driver produces exactly one tile at `0/0/0.pbf` and we
  * return its raw bytes. Intermediate state lives in a Java temp directory (not `/vsimem/`)
  * because `gdal.GetMemFileBuffer` only works for `FileFromMemBuffer`-created files — it
  * returns null for driver-written vsimem files, silently dropping the output. The temp
  * directory is recursively deleted before returning.
  *
  * Attribute fields carry native OGR value types: the field type is inferred from the first
  * non-null value's Scala runtime type (Int → `OFTInteger`, Long → `OFTInteger64`,
  * Double/Float → `OFTReal`, Boolean → `OFTInteger`+`OFSTBoolean` subtype, else `OFTString`),
  * matching the light `pyvx` tier. Field schema (the set of field names) is derived from the
  * first non-null attrs map.
  *
  * GDAL resource management (per "GDAL resource management" in CLAUDE.md): every
  * OGR `Feature` and `Geometry` allocated inside the loop is `.delete()`'d immediately,
  * the layer/datasource are closed via `ds.delete()`, and the temp directory tree is deleted
  * via `Files.walkFileTree` before returning.
  */
object MvtWriter {

    /** Default extent for a tile (4096 units = MVT v2 standard). */
    val DefaultExtent: Int = 4096

    /**
      * Encode features into a single MVT protobuf blob.
      *
      * @param layerName MVT layer name (e.g. "roads")
      * @param extent    Tile extent in pixels; defaults to 4096 (MVT v2)
      * @param features  Per-feature (WKB bytes, attrs Map[fieldName -> Any (native-typed)])
      * @return MVT protobuf bytes; empty Array[Byte] if no features were written
      *         (e.g. empty input or all geometries failed to parse).
      */
    def encode(
        layerName: String,
        extent: Int,
        features: Seq[(Array[Byte], Map[String, Any])]
    ): Array[Byte] = {
        ensureNativeLoaded()
        // Register OGR drivers once per JVM via the shared guard so concurrent MVT-encoding
        // tasks can't race the process-global driver registry. (init() needs an ExpressionConfig
        // not available on this static helper path; initOgr only registers OGR under the lock,
        // which is all this path needs — the native lib is already loaded above.)
        GDALManager.initOgr()
        val driver = GetDriverByName("MVT")
        if (driver == null) {
            throw new RuntimeException(
              "OGR MVT driver not found. Ensure GDAL is built with MVT driver support."
            )
        }

        // Use a real temp directory rather than /vsimem/ — gdal.GetMemFileBuffer only works
        // for files created via gdal.FileFromMemBuffer (returning null for driver-written files),
        // so vsimem-backed MVT creation output cannot be read back reliably. A real temp
        // directory is cleaned up via Files.walkFileTree after reading.
        //
        // The OGR MVT creation driver requires the target directory to NOT exist (it creates
        // the directory structure itself). We create a temp PARENT directory and let OGR create
        // the actual <root> subdirectory inside it; the parent is our cleanup root.
        val tmpParent: Path = Files.createTempDirectory("gbx_mvt_par_")
        val tmpRoot: Path = tmpParent.resolve("tile")
        val rootPath = tmpRoot.toAbsolutePath.toString

        // Create options: MAXZOOM=MINZOOM=0 → single tile at z/x/y = 0/0/0.
        val createOpts = new JVector[String]()
        createOpts.addAll(Seq(
          "MAXZOOM=0",
          "MINZOOM=0",
          "COMPRESS=NO",
          s"EXTENT=$extent",
          "FORMAT=DIRECTORY"
        ).asJava)

        val ds = driver.CreateDataSource(rootPath, createOpts)
        if (ds == null) {
            throw new RuntimeException(
              s"MVT driver failed to create datasource at $rootPath: ${gdal.GetLastErrorMsg()}"
            )
        }

        val srs = new SpatialReference()
        try {
            // EPSG:3857 is the canonical MVT projection — the driver expects this for its
            // tile-bound calculations even though we feed in tile-local coordinates.
            srs.ImportFromEPSG(3857)
            val layer = ds.CreateLayer(layerName, srs, wkbUnknown)
            if (layer == null) {
                throw new RuntimeException(s"Failed to create MVT layer '$layerName'")
            }

            // Derive field schema (names) from the first non-null attrs map. Use a stable
            // key ordering.
            val schema: Seq[String] = features
                .iterator
                .map(_._2)
                .find(_ != null)
                .map(_.keys.toSeq)
                .getOrElse(Seq.empty)

            // Infer each field's OGR type from the first non-null value across all features.
            schema.foreach { fieldName =>
                val firstVal = features
                    .iterator
                    .flatMap { case (_, attrs) => Option(attrs).flatMap(_.get(fieldName)) }
                    .find(_ != null)
                val (ogrType, subType): (Int, Option[Int]) = firstVal match {
                    case Some(_: Int) | Some(_: java.lang.Integer)     => (OFTInteger, None)
                    case Some(_: Long) | Some(_: java.lang.Long)       => (OFTInteger64, None)
                    case Some(_: Double) | Some(_: java.lang.Double)
                       | Some(_: Float) | Some(_: java.lang.Float)     => (OFTReal, None)
                    case Some(_: Boolean) | Some(_: java.lang.Boolean) => (OFTInteger, Some(OFSTBoolean))
                    case _                                             => (OFTString, None)
                }
                val fd = new FieldDefn(fieldName, ogrType)
                subType.foreach(fd.SetSubType)
                layer.CreateField(fd)
                fd.delete()
            }

            // Add each feature; pair every alloc with a delete() to avoid native-side leaks.
            features.foreach { case (wkb, attrs) =>
                if (wkb != null && wkb.nonEmpty) {
                    // GDAL 3.x can throw or return null on malformed WKB depending on
                    // exception-mode config — handle both so a single bad feature can't
                    // sink the whole tile.
                    // Map the WKB from tile-local [0, extent] into the web-mercator z0 world
                    // extent before parsing, so the driver's z0/EXTENT quantization round-trips
                    // it back to the original tile-local value. Done on the JTS geometry (the
                    // OGR Geometry API has no convenient per-coordinate affine), then re-WKB'd.
                    val worldWkb = Try(tileLocalToWorld(wkb, extent)).toOption.orNull
                    val geom = if (worldWkb == null) null
                               else Try(CreateGeometryFromWkb(worldWkb)).toOption.orNull
                    if (geom != null) {
                        val feat = new Feature(layer.GetLayerDefn())
                        try {
                            feat.SetGeometry(geom)
                            if (attrs != null) {
                                schema.foreach { fieldName =>
                                    attrs.get(fieldName).foreach { v =>
                                        setTypedField(feat, fieldName, v)
                                    }
                                }
                            }
                            layer.CreateFeature(feat)
                        } finally {
                            feat.delete()
                            geom.delete()
                        }
                    }
                }
            }

            // Reset any error state set by per-feature WKB-parse failures so that
            // SyncToDisk doesn't surface a stale CPL_ERROR_HANDLER message as a
            // RuntimeException when GDAL UseExceptions is enabled.
            gdal.ErrorReset()
            // SyncToDisk is best-effort: an empty or partially-failed layer can throw
            // (e.g. "OGR Error: General Error" on Sync) — we catch and let the /vsimem/
            // walk below decide whether any .pbf was actually produced.
            Try(layer.SyncToDisk())
            Try(ds.SyncToDisk())
        } finally {
            ds.delete()
            srs.delete()
        }

        // Find the .pbf file emitted by the MVT driver (at <root>/0/0/0.pbf with
        // MAXZOOM=MINZOOM=0). Read it with standard Java file I/O; delete the entire
        // temp directory tree afterward.
        val pbfFile = Paths.get(rootPath, "0", "0", "0.pbf")
        val bytes =
            if (Files.exists(pbfFile)) {
                Try(Files.readAllBytes(pbfFile)).getOrElse(Array.emptyByteArray)
            } else {
                Array.emptyByteArray
            }

        // Clean up the entire temp directory tree (including tmpParent).
        Try {
            Files.walkFileTree(tmpParent, new java.nio.file.SimpleFileVisitor[Path] {
                import java.nio.file.{FileVisitResult, attribute}
                override def visitFile(file: Path, attrs: attribute.BasicFileAttributes): FileVisitResult = {
                    Files.deleteIfExists(file)
                    FileVisitResult.CONTINUE
                }
                override def postVisitDirectory(dir: Path, exc: java.io.IOException): FileVisitResult = {
                    Files.deleteIfExists(dir)
                    FileVisitResult.CONTINUE
                }
            })
        }

        bytes
    }

    /**
      * Affine-map a WKB geometry from tile-local pixel space `[0, extent]` (origin upper-left,
      * y-down) into the full web-mercator z0 world extent (EPSG:3857 metres). With the layer SRS
      * set to EPSG:3857 and `MINZOOM=MAXZOOM=0`, the OGR MVT driver quantizes this single z0
      * world tile back onto the `[0, extent]` integer grid (and re-applies its own y-flip), so a
      * round-trip recovers the original tile-local coordinates (±1 from integer quantization).
      *
      *   world_x = WEBMERC_MIN + (u / extent) * worldSpan
      *   world_y = WEBMERC_MAX - (v / extent) * worldSpan   // v=0 (top) → WEBMERC_MAX (north)
      *
      * Returns the re-encoded WKB, or `null` if the input fails to parse.
      */
    private def tileLocalToWorld(wkb: Array[Byte], extent: Int): Array[Byte] = {
        val g = JTS.fromWKB(wkb)
        if (g == null || g.isEmpty) return null
        val worldSpan = TileMath.WEBMERC_MAX - TileMath.WEBMERC_MIN
        val coords = g.getCoordinates
        var i = 0
        while (i < coords.length) {
            val c = coords(i)
            val u = c.x
            val v = c.y
            c.x = TileMath.WEBMERC_MIN + (u / extent.toDouble) * worldSpan
            c.y = TileMath.WEBMERC_MAX - (v / extent.toDouble) * worldSpan
            i += 1
        }
        g.geometryChanged()
        JTS.toWKB(g)
    }

    /**
      * Set one OGR feature field using the typed setter that matches the value's Scala runtime
      * type, so the MVT encodes native int/long/double/bool values (not stringified). Long uses
      * the index-based `SetFieldInteger64` overload (no `(String, Long)` setter exists in the
      * SWIG bindings); booleans map to 0/1 (the field carries the `OFSTBoolean` subtype). Null
      * is skipped; any other type falls back to `toString`.
      */
    private def setTypedField(feat: Feature, fieldName: String, v: Any): Unit = v match {
        case null                                    => // skip
        case i: Int                                  => feat.SetField(fieldName, i)
        case i: java.lang.Integer                    => feat.SetField(fieldName, i.intValue)
        case l: Long                                 => feat.SetFieldInteger64(feat.GetFieldIndex(fieldName), l)
        case l: java.lang.Long                       => feat.SetFieldInteger64(feat.GetFieldIndex(fieldName), l.longValue)
        case d: Double                               => feat.SetField(fieldName, d)
        case d: java.lang.Double                     => feat.SetField(fieldName, d.doubleValue)
        case f: Float                                => feat.SetField(fieldName, f.toDouble)
        case f: java.lang.Float                      => feat.SetField(fieldName, f.doubleValue)
        case b: Boolean                              => feat.SetField(fieldName, if (b) 1 else 0)
        case b: java.lang.Boolean                    => feat.SetField(fieldName, if (b.booleanValue) 1 else 0)
        case other                                   => feat.SetField(fieldName, other.toString)
    }

    /**
      * Ensure the GDAL JNI shared library is loaded on this JVM (executor or driver).
      *
      * `ogr.RegisterAll()` and `ogr.GetDriverByName` both require `libgdalalljni.so`
      * to have been `System.load`-ed first, on the *executor* JVM before any OGR access,
      * not just on the driver. GDALManager loads it once per JVM (the same GDAL RasterX
      * uses); with no ExpressionConfig here, from `GBX_NATIVE_ROOT` or the system JNI.
      *
      * Package-private so `MvtDecoder` (same package) can call the shared guard
      * without duplicating the load path.
      */
    private[mvt] def ensureNativeLoaded(): Unit = GDALManager.ensureJniLoaded(None)

}
