package com.databricks.labs.gbx.expressions

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.adapters.SparkHadoopUtils
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, RuntimeReplaceable}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

/**
  * Case class (no fields): placeholder expression replaced at analysis time with a literal containing
  * the current [[ExpressionConfig]] serialized as base64. Used as an extra child in raster (and other)
  * expressions so that executors can obtain GDAL options, checkpoint dir, etc. without pulling from
  * SparkSession (which may not be available or consistent on executors).
  */
case class ExpressionConfigExpr() extends RuntimeReplaceable {

    /** Overrides Expression.dataType: StringType (base64 config). */
    override def dataType: DataType = StringType
    /** Overrides Expression.children: no children. */
    override def children: Seq[Expression] = Nil
    /** Overrides Expression.prettyName: "expr_config_expr". */
    override def prettyName: String = "expr_config_expr"

    /** Overrides Expression.withNewChildrenInternal: ignores nc, returns new ExpressionConfigExpr(). */
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = ExpressionConfigExpr()

    /** At analysis time, replace this with a literal holding ExpressionConfig.toB64 from the active session. */
    override lazy val replacement: Expression = {
        val activeSession = SparkSession.getActiveSession
        val defaultSession = SparkSession.getDefaultSession
        val fromSession = activeSession.orElse(defaultSession).map(s => ExpressionConfig(s).toB64)

        val b64 = fromSession.getOrElse {
            val conf = SparkEnv.get.conf
            val hConf = SparkHadoopUtils.sdu.newConfiguration(conf)
            new ExpressionConfig(conf.getAll.toMap, new SerializableConfiguration(hConf)).toB64
        }

        new Literal(UTF8String.fromString(b64), StringType) {
            override def toString(): String = "literal(configs[REDACTED])"
        }
    }

}
