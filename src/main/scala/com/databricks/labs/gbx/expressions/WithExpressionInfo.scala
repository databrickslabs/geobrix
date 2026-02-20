package com.databricks.labs.gbx.expressions

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

/**
  * Trait for registering a GeoBrix expression with Spark SQL.
  *
  * Implementations provide a SQL name (e.g. `gbx_rst_width`), a builder that constructs
  * the catalyst expression from child expressions, and optional metadata. Metadata for
  * DESCRIBE FUNCTION EXTENDED is loaded from `function-info.json` when present; the
  * default empty strings here are used only as fallbacks.
  *
  * Convention: the companion object of each expression case class extends this trait
  * and is registered via [[RegistryDelegate]] (e.g. from rasterx/gridx/vectorx functions).
  */
trait WithExpressionInfo {

    /** SQL function name (e.g. `gbx_rst_width`). Must be unique across all registered functions. */
    def name: String

    /** Builds the catalyst expression from the given child expressions. */
    def builder(): FunctionBuilder = {
        throw new IllegalAccessException("Builder not implemented")
    }

    // Reserved for future use with ExpressionConfig
    //    def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
    //        throw new IllegalAccessException("Builder not implemented")
    //    }

    /* ExpressionInfo(String className, String db, String name, String usage, String arguments, String examples,
                      String note, String group, String since, String deprecated, String source)
    */
    def info(): ExpressionInfo = new ExpressionInfo(
        getImplementingClassFullName,
        db,
        name,
        getUsage,
        getExtendedUsage,
        getExamples,
        note,
        group,
        since,
        deprecated,
        source
    )

    private val source: String = "built-in"

    /** Version when the function was added (e.g. 1.0); used in ExpressionInfo. */
    def since: String = "1.0"  // baseline

    private val db: String = "<default>"

    /** Short usage args for DESCRIBE FUNCTION (e.g. "tile_expr, band_index"); override in companion. */
    def usageArgs: String = ""

    /** One-line description for DESCRIBE FUNCTION; override in companion. */
    def description: String = ""

    /** Extended usage args for DESCRIBE FUNCTION EXTENDED; override when different from usageArgs. */
    def extendedUsageArgs: String = ""

    /** Example snippets for DESCRIBE FUNCTION EXTENDED; override or supply via function-info.json. */
    def examples: String = ""

    /** Extended description for DESCRIBE FUNCTION EXTENDED; override in companion. */
    def extendedDescription: String = ""

    /** Deprecation message if any; non-empty marks the function deprecated. */
    def deprecated: String = ""

    /** Optional note for DESCRIBE FUNCTION EXTENDED. */
    def note: String = ""

    /** Function group (e.g. "raster", "grid") for documentation. */
    def group: String = ""

    /** One-line usage string for DESCRIBE FUNCTION. */
    private def getUsage: String = s"${name}(${usageArgs}) - ${description}"

    /** Extended usage args or usageArgs if not overridden. */
    private def getExtendedUsageArgs: String = {
        if (extendedUsageArgs.isEmpty) {
            usageArgs
        } else {
            extendedUsageArgs
        }
    }

    /** Extended usage string for DESCRIBE FUNCTION. */
    private def getExtendedUsage: String = s"${name}(${getExtendedUsageArgs}) - ${extendedDescription}"

    /** Examples from FunctionInfoLoader or this.examples. */
    private def getExamples: String =
        FunctionInfoLoader
            .get(name)
            .flatMap(_.examples)
            .getOrElse(this.examples)

    /** Fully qualified class name of the implementing companion (for ExpressionInfo). */
    private def getImplementingClassFullName: String = {
        this.getClass.getName.replace("$", "") // fully qualified name of the implementing class
    }

}
