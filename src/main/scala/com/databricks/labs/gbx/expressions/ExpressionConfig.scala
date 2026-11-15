package com.databricks.labs.gbx.expressions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

/**
  * Case class: serializable snapshot of Spark and Hadoop config used when evaluating expressions on executors.
  * Raster (and other) expressions need GDAL options and other settings. Built on the driver from the
  * active session, serialized to base64 via toB64, and passed via [[ExpressionConfigExpr]] so executors
  * can reconstruct config for GDAL and other libraries.
  */
case class ExpressionConfig(
    configs: Map[String, String],
    hConf: SerializableConfiguration
) extends Serializable {

    /** Serialize to base64 for embedding in a catalyst literal (used by ExpressionConfigExpr). */
    def toB64: String = {
        val bos = new java.io.ByteArrayOutputStream()
        val oos = new java.io.ObjectOutputStream(bos)
        oos.writeObject(this); oos.close()
        java.util.Base64.getEncoder.encodeToString(bos.toByteArray)
    }

    /** Spark config entries that should be passed to GDAL as driver options. */
    def getGDALConfig: Map[String, String] = {
        configs.filter(p => {
            p._1.startsWith("spark.databricks.labs.gbx.gdal.") ||
            p._1.startsWith("spark.gdal.")
        })
    }

    /** Config entries for shared-object / native library paths. */
    def getSharedObjects: Map[String, String] = {
        configs.filter(p => {
            p._1.startsWith("spark.databricks.labs.gbx.sharedobjects.") ||
            p._1.startsWith("spark.sharedobjects.")
        })
    }

    /** The env wheel's `_native/<arch>` directory whose GDAL this JVM loads (see GDALManager.resolveNative). */
    def nativeDir: Option[String] = configs.get(com.databricks.labs.gbx.rasterx.gdal.GDALManager.NativeDirKey)

    /** The decoded size, in bytes, a materialized tile may have where its bytes are read (the session's
      * `spark.databricks.labs.gbx.read_cap`, which the Python manager sets); None when it is not set or not a number. */
    def readCap: Option[Long] =
        configs.get(com.databricks.labs.gbx.rasterx.gdal.TileBytes.ReadCapKey).flatMap(_.trim.toLongOption).filter(_ > 0)

    /** If true, expression errors surface as exceptions; if false, return null and optionally log. */
    def crashExpressions: Boolean = {
        configs.getOrElse("spark.databricks.labs.gbx.expressions.crash.on.error", "false").toBoolean
    }

}

object ExpressionConfig {

    /** Build config from the given Spark session (driver-side).
      *
      * Merges [[com.databricks.labs.gbx.operations.ProjGridRegistry]] dirs under the synthetic
      * key `spark.databricks.labs.gbx.gdal.PROJ_GRID_DIRS` (colon-joined) so that executors
      * receive them via the serialized ExpressionConfig and GDALManager can prepend them to the
      * PROJ search path at init time.  The key uses the `spark.databricks.labs.gbx.gdal.`
      * prefix so it travels with the rest of the GDAL config; GDALManager's generic config loop
      * explicitly skips it to avoid setting a bogus GDAL option.
      */
    def apply(spark: SparkSession): ExpressionConfig = {
        val base = spark.conf.getAll
        val gridDirs = com.databricks.labs.gbx.operations.ProjGridRegistry.get
        val merged =
          if (gridDirs.isEmpty) base
          else base + ("spark.databricks.labs.gbx.gdal.PROJ_GRID_DIRS" -> gridDirs.mkString(":"))
        new ExpressionConfig(merged, new SerializableConfiguration(spark.sessionState.newHadoopConf()))
    }

    /** The configurations this JVM has decoded, by their encoding. A row-level expression (`gbx_st_transformcrs`)
      * calls [[fromB64]] once per ROW, and a decode deserializes the whole Spark conf map and Hadoop
      * `Configuration` (~1 ms; the transform itself is ~10 us), so each distinct encoding is decoded once per JVM.
      * A session has one, so the map stays as small as the sessions that used the engine here; the cap is a
      * backstop for a long-lived JVM that cycles through many. */
    private val decoded = new java.util.concurrent.ConcurrentHashMap[String, ExpressionConfig]()
    private val DecodedCap = 64

    /** Deserialize from base64 (used on executors); decoded once per JVM per distinct encoding. The result is a
      * shared, read-only value: callers read it and never mutate its `hConf`. */
    def fromB64(b64: String): ExpressionConfig = {
        val hit = decoded.get(b64)
        if (hit != null) hit
        else {
            val bytes = java.util.Base64.getDecoder.decode(b64)
            val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes))
            val cfg = try ois.readObject().asInstanceOf[ExpressionConfig] finally ois.close()
            if (decoded.size() >= DecodedCap) decoded.clear()
            decoded.put(b64, cfg)
            cfg
        }
    }

    /** The last (literal instance, decoded config) pair [[fromUtf8]] saw. */
    @volatile private var lastLiteral: (UTF8String, ExpressionConfig) = _

    /** [[fromB64]] for the per-row path. The config arrives as the SAME `UTF8String` literal instance on every row,
      * so a reference hit skips even `conf.toString` and the hash of its ~50 KB (each ~40 us, still 4x the
      * transform); a different instance falls back to [[fromB64]]'s per-encoding cache. */
    def fromUtf8(conf: UTF8String): ExpressionConfig = {
        val last = lastLiteral
        if (last != null && (last._1 eq conf)) last._2
        else {
            val cfg = fromB64(conf.toString)
            lastLiteral = (conf, cfg)
            cfg
        }
    }

    /** Decode config from an expression that evaluated to a config literal (e.g. ExpressionConfigExpr). */
    def fromExpr(expr: Expression): ExpressionConfig = {
        val b64 = expr.eval(null).asInstanceOf[UTF8String]
        fromB64(b64.toString)
    }

}
