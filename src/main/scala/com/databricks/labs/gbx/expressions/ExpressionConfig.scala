package com.databricks.labs.gbx.expressions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

/**
  * Case class: serializable snapshot of Spark and Hadoop config used when evaluating expressions on executors.
  * Raster (and other) expressions need GDAL options, checkpoint dir, etc. Built on the driver from the
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

    /** Directory for raster checkpoint files when useCheckpoint is true. */
    def getRasterCheckpointDir: String = {
        configs.getOrElse("spark.databricks.labs.gbx.raster.checkpoint.dir", "/tmp/raster-checkpoint")
    }

    /** Whether to checkpoint intermediate rasters to getRasterCheckpointDir. */
    def useCheckpoint: Boolean = {
        configs.getOrElse("spark.databricks.labs.gbx.raster.use.checkpoint", "false").toBoolean
    }

    /** If true, expression errors surface as exceptions; if false, return null and optionally log. */
    def crashExpressions: Boolean = {
        configs.getOrElse("spark.databricks.labs.gbx.expressions.crash.on.error", "false").toBoolean
    }

}

object ExpressionConfig {

    /** Build config from the given Spark session (driver-side). */
    def apply(spark: SparkSession): ExpressionConfig = {
        new ExpressionConfig(
          spark.conf.getAll,
          new SerializableConfiguration(spark.sessionState.newHadoopConf())
        )
    }

    /** Deserialize from base64 (used on executors). */
    def fromB64(b64: String): ExpressionConfig = {
        val bytes = java.util.Base64.getDecoder.decode(b64)
        val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes))
        ois.readObject().asInstanceOf[ExpressionConfig]
    }

    /** Decode config from an expression that evaluated to a config literal (e.g. ExpressionConfigExpr). */
    def fromExpr(expr: Expression): ExpressionConfig = {
        val b64 = expr.eval(null).asInstanceOf[UTF8String]
        fromB64(b64.toString)
    }

}
