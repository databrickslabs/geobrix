package com.databricks.labs.gbx.expressions

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry

/**
  * Case class: holds a Spark FunctionRegistry and optional name prefix. Used by rasterx/gridx/vectorx
  * functions to register expression companions (SQL name, info, builder). Each registered function
  * uses [[WithExpressionInfo.info]] for DESCRIBE and [[WithExpressionInfo.builder]] to construct the catalyst expression.
  */
case class RegistryDelegate(registry: FunctionRegistry, prefix: Option[String] = None) {

    /** Registry name: prefix_name if prefix is set, else companion.name. */
    private def getName(companion: WithExpressionInfo): String = {
        prefix.map(p => s"${p}_${companion.name}").getOrElse(companion.name)
    }

    /** Registers the given expression companion with the function registry (SQL name, info, builder). */
    def register(companion: WithExpressionInfo): Unit = {
        val name = getName(companion)
        registry.registerFunction(
          FunctionIdentifier(name),
          companion.info(),
          companion.builder()
        )
    }

    // For future use with ExpressionConfig
    //    def register(companion: WithExpressionInfo, ec: ExpressionConfig): Unit = {
    //        val name = getName(companion)
    //        registry.registerFunction(
    //          FunctionIdentifier(name),
    //          companion.builder(ec),
    //          source = "built-in"
    //        )
    //    }

}
