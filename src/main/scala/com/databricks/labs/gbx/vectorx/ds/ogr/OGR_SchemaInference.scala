package com.databricks.labs.gbx.vectorx.ds.ogr

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.ogr._

import java.sql.Timestamp
import scala.util.Try

/** Infers Spark schema from OGR layer/features and maps OGR types to Spark types. */
//noinspection VarCouldBeVal
object OGR_SchemaInference extends Serializable {

    /** Creates an empty OGR geometry (POINT EMPTY) for type inference; side-effect registers drivers. */
    private def OGREmptyGeometry: Geometry = {
        enableOGRDrivers()
        ogr.CreateGeometryFromWkt("POINT EMPTY")
    }

    /** Registers all OGR drivers once per JVM via the process-wide guard (thread-safe). */
    final def enableOGRDrivers(force: Boolean = false): Unit = {
        if (force) {
            // Explicit force re-registration (rare; e.g. tests). RegisterAll is additive/idempotent.
            ogr.RegisterAll()
        } else {
            GDALManager.initOgr()
        }
    }

    /** Maps OGR field type name to Spark DataType (e.g. Integer -> IntegerType, Real -> DoubleType). */
    def getType(typeName: String): DataType =
        typeName match {
            case "Boolean"        => BooleanType
            case "Integer"        => IntegerType
            case "String"         => StringType
            case "Real"           => DoubleType
            case "Date"           => DateType
            case "Time"           => StringType // Spark has no TIME type: GDAL's HH:MM:SS[.sss] text
            case "DateTime"       => TimestampType
            case "Binary"         => BinaryType
            case "IntegerList"    => ArrayType(IntegerType)
            case "RealList"       => ArrayType(DoubleType)
            case "StringList"     => ArrayType(StringType)
            case "WideString"     => StringType
            case "WideStringList" => ArrayType(StringType)
            case "Integer64"      => LongType
            case _                => StringType
        }

    /** Spark type of a field: its declared OGR type, with the Boolean subtype read as
      * booleans (as the lightweight readers do). */
    def fieldType(field: FieldDefn): DataType =
        if (field.GetSubType == ogrConstants.OFSTBoolean) BooleanType
        else getType(field.GetFieldTypeName(field.GetFieldType))

    /** Extracts feature field j as Spark type (dataType); dates/timestamps via getDate/getDateTime.
      * Unset and NULL fields are null (OGR's GetFieldAs* would return 0 / "" for them). */
    def getValue(feature: Feature, j: Int, dataType: DataType): Any = {
        if (!feature.IsFieldSetAndNotNull(j)) return null
        dataType match {
            case BooleanType               => feature.GetFieldAsInteger(j) != 0
            case IntegerType               => feature.GetFieldAsInteger(j)
            case LongType                  => feature.GetFieldAsInteger64(j)
            case StringType                => feature.GetFieldAsString(j)
            case DoubleType                => feature.GetFieldAsDouble(j)
            case DateType                  => getDate(feature, j)
            case TimestampType             => getDateTime(feature, j)
            case BinaryType                => feature.GetFieldAsBinary(j)
            case ArrayType(IntegerType, _) => feature.GetFieldAsIntegerList(j)
            case ArrayType(DoubleType, _)  => feature.GetFieldAsDoubleList(j)
            case ArrayType(StringType, _)  => feature.GetFieldAsStringList(j)
            case _                         => feature.GetFieldAsString(j)
        }
    }

    /** Returns the 0-based field index for the given field name, or None if not found. */
    def getFieldIndex(feature: Feature, name: String): Option[Int] = {
        val field = feature.GetFieldDefnRef(name)
        if (field == null) None
        else (0 until feature.GetFieldCount).find(i => feature.GetFieldDefnRef(i).GetName == name)
    }

    /** OGR DateTime field -> java.sql.Timestamp; returns null for invalid components. */
    // noinspection ScalaDeprecation
    private def getJavaSQLTimestamp(feature: Feature, id: Int): Timestamp = {
        var year: Array[Int] = Array.fill[Int](1)(0)
        var month: Array[Int] = Array.fill[Int](1)(0)
        var day: Array[Int] = Array.fill[Int](1)(0)
        var hour: Array[Int] = Array.fill[Int](1)(0)
        var minute: Array[Int] = Array.fill[Int](1)(0)
        var second: Array[Float] = Array.fill[Float](1)(0)
        var tz: Array[Int] = Array.fill[Int](1)(0)
        feature.GetFieldAsDateTime(id, year, month, day, hour, minute, second, tz)

        val y = year(0)
        val m = month(0)
        val d = day(0)
        val H = hour(0)
        val M = minute(0)
        val s = second(0)

        // Validate date components - GDAL/OGR may return invalid values (e.g., month=0)
        // If invalid, return null timestamp instead of throwing exception
        if (y < 1 || m < 1 || m > 12 || d < 1 || d > 31 || H < 0 || H > 23 || M < 0 || M > 59) {
            return null
        }

        val sInt = math.floor(s).toInt
        val nanos = ((s - sInt) * 1e9).round.toInt.max(0)

        try {
            val ldt = java.time.LocalDateTime.of(y, m, d, H, M, sInt, nanos)

            val inst = tz(0) match {
                case 100   => ldt.toInstant(java.time.ZoneOffset.UTC) // UTC
                case 0 | 1 => ldt.atZone(java.time.ZoneId.systemDefault()).toInstant // unknown/local
                case off   => ldt.atOffset(java.time.ZoneOffset.ofTotalSeconds(off * 60)).toInstant // minutes offset
            }

            val ts: java.sql.Timestamp = java.sql.Timestamp.from(inst)
            ts
        } catch {
            // Handle any remaining invalid date/time combinations (e.g., Feb 30)
            case _: java.time.DateTimeException => null
        }
    }

    /** Date field as Spark DateType (days since epoch); null if invalid. */
    private def getDate(feature: Feature, id: Int): Any = {
        val timestamp = getJavaSQLTimestamp(feature, id)
        if (timestamp == null) {
            // Return null for invalid dates (e.g., month=0, day=0)
            return null
        }
        val localDate = timestamp.toLocalDateTime.toLocalDate       // correct Y-M-D
        val date = java.sql.Date.valueOf(localDate)                 // build sql.Date properly
        DateTimeUtils.fromJavaDate(date)
    }

    /** DateTime field as Spark TimestampType (micros); null if invalid. */
    private def getDateTime(feature: Feature, id: Int): Any = {
        val datetime = getJavaSQLTimestamp(feature, id)
        if (datetime == null) {
            // Return null for invalid datetimes (e.g., month=0, day=0)
            return null
        }
        DateTimeUtils.fromJavaTimestamp(datetime)
    }

    /** Builds StructType from the declared OGR field types plus geom fields (WKT/WKB + srid + proj4). */
    private def getFeatureSchema(feature: Feature, asWKB: Boolean): StructType = {
        val geomDataType = if (asWKB) BinaryType else StringType
        val fields = (0 until feature.GetFieldCount())
            .map(j => {
                val field = feature.GetFieldDefnRef(j)
                val name = field.GetNameRef
                val fieldName = if (name.isEmpty) f"field_$j" else name
                StructField(fieldName, fieldType(field))
            }) ++ (0 until feature.GetGeomFieldCount())
            .flatMap(j => {
                val field = feature.GetGeomFieldDefnRef(j)
                val name = field.GetNameRef
                val geomName = if (name.isEmpty) f"geom_$j" else name
                Seq(
                  StructField(geomName, geomDataType),
                  StructField(geomName + "_srid", StringType),
                  StructField(geomName + "_srid_proj", StringType)
                )
            })
        StructType(fields)
    }

    /** Values for all feature fields and geom fields (WKT/WKB, srid, proj4) in schema order. */
    def getFeatureFields(feature: Feature, featureSchema: StructType, asWKB: Boolean): Array[Any] = {
        val types = featureSchema.fields.map(_.dataType)
        val fields = (0 until feature.GetFieldCount())
            .map(j => getValue(feature, j, types(j)))
        val geoms = (0 until feature.GetGeomFieldCount())
            .map(feature.GetGeomFieldRef)
            .flatMap(f => {
                if (Option(f).isDefined) {
                    // f.FlattenTo2D()
                    Seq(
                      if (asWKB) f.ExportToWkb else f.ExportToWkt,
                      Try(f.GetSpatialReference.GetAuthorityCode(null)).getOrElse("0"),
                      Try(f.GetSpatialReference.ExportToProj4).getOrElse("")
                    )
                } else {
                    Seq(
                      if (asWKB) OGREmptyGeometry.ExportToWkb else OGREmptyGeometry.ExportToWkt,
                      "0",
                      ""
                    )
                }

            })
        val values = fields ++ geoms
        values.toArray
    }

    /** Opens layer at path with driverName and options; returns Some(schema) from the first feature's
      * declared field types, or None for an empty layer. */
    def inferSchemaImpl(
        driverName: String,
        path: String,
        options: Map[String, String]
    ): Option[StructType] = {
        GDALManager.initOgr()
        val layerN = options.getOrElse("layerNumber", "0").toInt
        val layerName = options.getOrElse("layerName", "")
        val asWKB = options.getOrElse("asWKB", "true").toBoolean

        val dataset = OGR_Driver.open(path, driverName)
        val layerByIndex = dataset.GetLayer(layerN)
        if (layerByIndex == null) {
            throw new IllegalArgumentException(
              s"No layer at index $layerN in dataset. Path: $path"
            )
        }
        val resolvedLayerName = if (layerName.isEmpty) layerByIndex.GetName() else layerName
        val layer = dataset.GetLayer(resolvedLayerName)
        if (layer == null) {
            throw new IllegalArgumentException(
              s"Layer '$resolvedLayerName' not found in dataset. Path: $path. " +
              s"Available layers: ${(0 until dataset.GetLayerCount()).map(i => dataset.GetLayer(i).GetName()).mkString(", ")}"
            )
        }
        layer.ResetReading()
        val headFeature = layer.GetNextFeature()

        // Handle empty layers - return None if no features
        if (headFeature == null) {
            return None
        }

        // Field types are the layer's declared OGR types, identical for every feature.
        Some(getFeatureSchema(headFeature, asWKB))

    }

}
