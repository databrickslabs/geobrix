package com.databricks.labs.gbx.vectorx.ds.ogr

import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
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

    /** Registers all OGR drivers if they haven't been registered yet. */
    final def enableOGRDrivers(force: Boolean = false): Unit = {
        val drivers = ogr.GetDriverCount
        if (drivers == 0 || force) {
            ogr.RegisterAll()
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
            case "Time"           => TimestampType
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

    /** Infers Spark type from feature field value; coerces to reported OGR type when not String. */
    private def inferType(feature: Feature, j: Int): DataType = {
        val field = feature.GetFieldDefnRef(j)
        val reportedType = getType(field.GetFieldTypeName(field.GetFieldType))
        val value = feature.GetFieldAsString(j)

        if (value == "") {
            reportedType
        } else {
            val coerceables = Seq(
              (Try(value.toInt).toOption, IntegerType),
              (Try(value.toDouble).toOption, DoubleType),
              (Try(value.toBoolean).toOption, BooleanType),
              (Try(value.toLong).toOption, LongType),
              (Try(value.toFloat).toOption, FloatType),
              (Try(value.toShort).toOption, ShortType),
              (Try(value.toByte).toOption, ByteType),
              (Try(DateTimeUtils.stringToDate(UTF8String.fromString(value)).get).toOption, DateType),
              (Try(DateTimeUtils.stringToTimestampWithoutTimeZone(UTF8String.fromString(value)).get).toOption, TimestampType)
            ).filter(_._1.isDefined).map(_._2)

            val inferredType = coerceTypeList(coerceables)
            if (reportedType == StringType && inferredType != StringType) inferredType
            else TypeCoercion.findTightestCommonType(reportedType, inferredType).getOrElse(StringType)
        }
    }

    /** Picks single DataType from list (precedence: String > Long > Double > ... > Date > String fallback). */
    private[ds] def coerceTypeList(coerceables: Seq[DataType]): DataType = {
        if (coerceables.isEmpty) StringType
        else if (coerceables.contains(StringType)) StringType
        else if (coerceables.contains(LongType)) LongType
        else if (coerceables.contains(DoubleType)) DoubleType
        else if (coerceables.contains(FloatType)) FloatType
        else if (coerceables.contains(IntegerType)) IntegerType
        else if (coerceables.contains(ShortType)) ShortType
        else if (coerceables.contains(BinaryType)) BinaryType
        else if (coerceables.contains(ByteType)) ByteType
        else if (coerceables.contains(BooleanType)) BooleanType
        else if (coerceables.contains(TimestampType)) TimestampType
        else if (coerceables.contains(DateType)) DateType
        else StringType
    }

    /** Extracts feature field j as Spark type (dataType); dates/timestamps via getDate/getDateTime. */
    def getValue(feature: Feature, j: Int, dataType: DataType): Any = {
        dataType match {
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

    /** Builds StructType from feature fields (inferType) plus geom fields (WKT/WKB + srid + proj4). */
    private def getFeatureSchema(feature: Feature, asWKB: Boolean): StructType = {
        val geomDataType = if (asWKB) BinaryType else StringType
        val fields = (0 until feature.GetFieldCount())
            .map(j => {
                val field = feature.GetFieldDefnRef(j)
                val name = field.GetNameRef
                val fieldName = if (name.isEmpty) f"field_$j" else name
                StructField(fieldName, inferType(feature, j))
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

    /** Opens layer at path with driverName and options; returns Some(schema) from first feature or None. */
    def inferSchemaImpl(
        driverName: String,
        path: String,
        options: Map[String, String]
    ): Option[StructType] = {
        ogr.RegisterAll()
        val layerN = options.getOrElse("layerNumber", "0").toInt
        val layerName = options.getOrElse("layerName", "")
        val inferenceLimit = options.getOrElse("inferenceLimit", "100").toInt
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

        val headSchemaFields = getFeatureSchema(headFeature, asWKB).fields
        val n = math.min(inferenceLimit, layer.GetFeatureCount()).toInt

        // start from 1 since 1 feature was read already
        val layerSchema = (1 until n).foldLeft(headSchemaFields) { (schema, _) =>
            val feature = layer.GetNextFeature()

            // Skip null features (shouldn't happen but defensive programming)
            if (feature == null) {
                schema
            } else {
                val featureSchema = getFeatureSchema(feature, asWKB)
                schema.zip(featureSchema.fields).map { case (s, f) =>
                    (s, f) match {
                        case (StructField(name, StringType, _, _), StructField(_, dataType, _, _)) =>
                            StructField(name, dataType, nullable = true)
                        case (StructField(name, dataType, _, _), StructField(_, StringType, _, _)) =>
                            StructField(name, dataType, nullable = true)
                        case (StructField(name, dataType, _, _), StructField(_, dataType2, _, _))  =>
                            StructField(name, TypeCoercion.findTightestCommonType(dataType2, dataType).getOrElse(StringType), nullable = true)
                    }
                }
            }
        }

        Some(StructType(layerSchema))

    }

}
