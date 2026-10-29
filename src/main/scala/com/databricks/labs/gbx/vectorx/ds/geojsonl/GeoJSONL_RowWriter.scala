package com.databricks.labs.gbx.vectorx.ds.geojsonl

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.util.HadoopUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration
import org.gdal.ogr.ogr.{CreateGeometryFromWkb, GetDriverByName}
import org.gdal.ogr.ogrConstants._
import org.gdal.ogr.{Feature, FieldDefn, Geometry}
import org.gdal.osr.SpatialReference

import java.nio.file.Files
import java.util.Locale
import scala.collection.mutable
import scala.util.Try

/**
  * Per-task data writer for the `geojsonl_ogr` DataSource.
  *
  * Behavior:
  *   - Buffer the partition's rows (geometry as WKB bytes - already WKB, or WKT parsed to WKB -
  *     plus attribute values).
  *   - Flush a shard when the buffer reaches `maxRecordsPerFile` (if set) and once more at the end.
  *   - Each flush encodes a `GeoJSONSeq` shard to worker-local temp via OGR (one `Feature` per line),
  *     then copies it into the output directory as `part-<uuid>.geojsonl` and deletes the temp file.
  *   - Returns a [[GeoJSONL_WriterMsg]] listing the shard paths written.
  *
  * Shard names use a fresh UUID per flush, so they are unique across tasks AND across a single
  * partition's chunks (matching the lightweight writer). GDAL/OGR registration goes through the
  * synchronized `GDALManager.initOgr()` guard (REQUIRED - never raw `RegisterAll` per task).
  */
class GeoJSONL_RowWriter(
    schema: StructType,
    outPath: String,
    options: Map[String, String],
    hConf: SerializableConfiguration
) extends DataWriter[InternalRow] {

    private val ciOptions = options.map { case (k, v) => k.toLowerCase(Locale.ROOT) -> v }
    private val maxRecordsPerFile: Int = ciOptions.get("maxrecordsperfile").map(_.toInt).getOrElse(0)
    if (maxRecordsPerFile < 0) {
        throw new IllegalArgumentException("maxRecordsPerFile must be a non-negative integer.")
    }
    private val geometryTypeOverride: Option[String] = ciOptions.get("geometrytype")
    private val layerNameOpt: Option[String] = ciOptions.get("layername")
    private val geomColOpt: Option[String] = ciOptions.get("geomcol")
    private val sridColOpt: Option[String] = ciOptions.get("sridcol")
    private val projColOpt: Option[String] = ciOptions.get("projcol")

    private val roles = GeoJSONL_DataSource.resolveRoles(schema, geomColOpt, sridColOpt, projColOpt)
    private val geomIdx = schema.fieldIndex(roles.geomCol)
    private val sridIdx = schema.fieldIndex(roles.sridCol)
    private val projIdx = if (schema.fieldNames.contains(roles.projCol)) schema.fieldIndex(roles.projCol) else -1
    private val attrIdx: Seq[(String, Int, DataType)] =
        roles.attrCols.map(c => (c, schema.fieldIndex(c), schema(c).dataType))

    private val outDirClean = HadoopUtils.cleanPath(outPath)
    private val outDirPath = new Path(outDirClean)
    private val fs = outDirPath.getFileSystem(hConf.value)

    // Buffered features: (wkb, attrs). srid/proj captured from the first row that has them.
    private val buffer = mutable.ArrayBuffer.empty[(Array[Byte], Array[Any])]
    private var sridCode: String = "0"
    private var proj4: String = ""
    private var sridSeen: Boolean = false

    private val shardPaths = mutable.ArrayBuffer.empty[String]

    /** Buffer one row; geometry coerced to WKB (already WKB, or WKT -> WKB); flush at the boundary. */
    override def write(row: InternalRow): Unit = {
        val wkb: Array[Byte] =
            if (row.isNullAt(geomIdx)) null
            else if (roles.geomIsWkb) row.getBinary(geomIdx)
            else wktToWkb(row.getUTF8String(geomIdx).toString)

        if (!sridSeen) {
            if (!row.isNullAt(sridIdx)) {
                sridCode = row.getUTF8String(sridIdx).toString
            }
            if (projIdx >= 0 && !row.isNullAt(projIdx)) {
                proj4 = row.getUTF8String(projIdx).toString
            }
            sridSeen = true
        }

        val attrs = attrIdx.map { case (_, idx, dt) => extractAttr(row, idx, dt) }.toArray
        buffer += ((wkb, attrs))

        if (maxRecordsPerFile > 0 && buffer.length >= maxRecordsPerFile) flushShard()
    }

    /** Final flush of any buffered rows, then return the list of shard paths written. */
    override def commit(): WriterCommitMessage = {
        if (buffer.nonEmpty) flushShard()
        GeoJSONL_WriterMsg(shardPaths.toSeq)
    }

    /** Best-effort delete of any shards already published by this task. */
    override def abort(): Unit = {
        shardPaths.foreach { p =>
            try fs.delete(new Path(p), false) catch { case _: Throwable => () }
        }
    }

    /** No streaming handle held open between flushes; nothing to release. */
    override def close(): Unit = ()

    /** Encode the buffered features to a GeoJSONSeq shard, copy into the output dir, clear buffer. */
    private def flushShard(): Unit = {
        if (buffer.isEmpty) return
        GeoJSONL_RowWriter.ensureNativeLoaded()
        GDALManager.initOgr()
        val driver = GetDriverByName("GeoJSONSeq")
        if (driver == null) {
            throw new RuntimeException(
                "OGR GeoJSONSeq driver not found. Ensure GDAL is built with GeoJSONSeq driver support.")
        }

        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val shardName = s"part-$uuid.geojsonl"
        val tmpDir = Files.createTempDirectory("gbx_geojsonl_")
        val localShard = tmpDir.resolve(shardName)

        val srs = buildSrs()
        try {
            val ds = driver.CreateDataSource(localShard.toString)
            if (ds == null) {
                throw new RuntimeException(
                    s"GeoJSONSeq driver failed to create datasource at $localShard: " +
                    org.gdal.gdal.gdal.GetLastErrorMsg())
            }
            try {
                val geomType = inferGeomType(srs)
                val layer = ds.CreateLayer(layerNameOpt.getOrElse(roles.geomCol), srs, geomType)
                if (layer == null) {
                    throw new RuntimeException("Failed to create GeoJSONSeq layer.")
                }
                attrIdx.foreach { case (name, _, dt) =>
                    val fd = new FieldDefn(name, ogrFieldType(dt))
                    layer.CreateField(fd)
                    fd.delete()
                }
                val defn = layer.GetLayerDefn()
                buffer.foreach { case (wkb, attrs) =>
                    val feat = new Feature(defn)
                    try {
                        attrIdx.zipWithIndex.foreach { case ((name, _, _), i) =>
                            val v = attrs(i)
                            if (v != null) setField(feat, name, v)
                        }
                        if (wkb != null && wkb.nonEmpty) {
                            val geom = Try(CreateGeometryFromWkb(wkb)).toOption.orNull
                            if (geom != null) {
                                try feat.SetGeometry(geom) finally geom.delete()
                            }
                        }
                        layer.CreateFeature(feat)
                    } finally feat.delete()
                }
            } finally ds.delete()

            // Copy the encoded shard into the output directory (sequential -> FUSE-safe on Volumes).
            HadoopUtils.copyToPath(localShard.toString, s"$outDirClean/$shardName", hConf)
            shardPaths += new Path(outDirPath, shardName).toString
        } finally {
            if (srs != null) srs.delete()
            try Files.deleteIfExists(localShard) catch { case _: Throwable => () }
            try Files.deleteIfExists(tmpDir) catch { case _: Throwable => () }
            buffer.clear()
        }
    }

    /** Build the layer SRS from the captured srid code (EPSG) or PROJ4 fallback; null if CRS-less. */
    private def buildSrs(): SpatialReference = {
        if (sridCode != null && sridCode.nonEmpty && sridCode != "0") {
            val srs = new SpatialReference()
            Try(srs.ImportFromEPSG(sridCode.toInt)) match {
                case scala.util.Success(_) => srs
                case scala.util.Failure(_) => srs.delete(); fromProj4()
            }
        } else fromProj4()
    }

    private def fromProj4(): SpatialReference = {
        if (proj4 != null && proj4.nonEmpty) {
            val srs = new SpatialReference()
            Try(srs.ImportFromProj4(proj4)) match {
                case scala.util.Success(_) => srs
                case scala.util.Failure(_) => srs.delete(); null
            }
        } else null
    }

    /** Geometry type: explicit override (name), else the first non-null feature's WKB type, else unknown. */
    private def inferGeomType(srs: SpatialReference): Int = {
        geometryTypeOverride.map(geomTypeFromName).getOrElse {
            buffer.iterator
                .map(_._1)
                .filter(w => w != null && w.nonEmpty)
                .flatMap(w => Option(Try(CreateGeometryFromWkb(w)).toOption.orNull))
                .map { g => val t = g.GetGeometryType; g.delete(); t }
                .nextOption()
                .getOrElse(wkbUnknown)
        }
    }

    private def wktToWkb(wkt: String): Array[Byte] = {
        if (wkt == null) return null
        val g: Geometry = Try(org.gdal.ogr.ogr.CreateGeometryFromWkt(wkt)).toOption.orNull
        if (g == null) null else { val b = g.ExportToWkb; g.delete(); b }
    }

    /** Extract an attribute value as a JVM type OGR SetField accepts. */
    private def extractAttr(row: InternalRow, idx: Int, dt: DataType): Any = {
        if (row.isNullAt(idx)) return null
        dt match {
            case IntegerType   => row.getInt(idx)
            case LongType      => row.getLong(idx)
            case ShortType     => row.getShort(idx).toInt
            case ByteType      => row.getByte(idx).toInt
            case DoubleType    => row.getDouble(idx)
            case FloatType     => row.getFloat(idx).toDouble
            case BooleanType   => if (row.getBoolean(idx)) 1 else 0
            case StringType    => row.getUTF8String(idx).toString
            case _             => row.get(idx, dt).toString
        }
    }

    private def setField(feat: Feature, name: String, v: Any): Unit = v match {
        case i: Int     => feat.SetField(name, i)
        case l: Long    => feat.SetField(name, l.toString) // OGR Java has no long overload; widen via string
        case d: Double  => feat.SetField(name, d)
        case s: String  => feat.SetField(name, s)
        case other      => feat.SetField(name, other.toString)
    }

    private def ogrFieldType(dt: DataType): Int = dt match {
        case IntegerType | ShortType | ByteType | BooleanType => OFTInteger
        case LongType                                         => OFTInteger64
        case DoubleType | FloatType                           => OFTReal
        case BinaryType                                       => OFTBinary
        case _                                                => OFTString
    }

    private def geomTypeFromName(name: String): Int = name.toLowerCase(Locale.ROOT) match {
        case "point"              => wkbPoint
        case "linestring"         => wkbLineString
        case "polygon"            => wkbPolygon
        case "multipoint"         => wkbMultiPoint
        case "multilinestring"    => wkbMultiLineString
        case "multipolygon"       => wkbMultiPolygon
        case "geometrycollection" => wkbGeometryCollection
        case _                    => wkbUnknown
    }
}

object GeoJSONL_RowWriter {

    /**
      * Ensure the GDAL JNI shared library is loaded on this JVM before any OGR access. Mirrors
      * `MvtWriter.ensureNativeLoaded` - `GetDriverByName` / `RegisterAll` require
      * `libgdalalljni.so` to be `System.load`-ed first, and it must happen on the executor JVM.
      * GDALManager loads it once per JVM, the same GDAL RasterX uses.
      */
    private[geojsonl] def ensureNativeLoaded(): Unit = GDALManager.ensureJniLoaded(None)
}
