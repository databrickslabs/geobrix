package com.databricks.labs.gbx.kernels

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.kernels.spec.{GdalSpecArgv, GdalSpecExpr, GdalSpecs, RasterProps, SpecDataException, SpecFunction}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Generator, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, OneRowRelation}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

import scala.util.{Failure, Try}

/** The manager's L1 primitive: install ONE companion under the name the manifest gives.
  * Scala decides nothing here; the Python registrar says which names exist and at which level. */
object L1Installer {

    private def companion(fqn: String): WithExpressionInfo = {
        // scalastyle:off classforname
        val cls = Class.forName(fqn + "$")
        // scalastyle:on classforname
        cls.getField("MODULE$").get(null).asInstanceOf[WithExpressionInfo]
    }

    /** The arities in [min, max] the companion's builder accepts, as a comma list.
      * A builder that reads too few children fails (MatchError, IndexOutOfBounds, "expects N arguments");
      * one that ignores extra children still counts them, so the manager passes the manifest's own range. */
    def acceptedArities(fqn: String, min: Int, max: Int): String = {
        val b = companion(fqn).builder()
        (min to max).filter { n =>
            Try(b(Seq.fill(n)(Literal(null)))) match {
                case Failure(_: MatchError | _: IndexOutOfBoundsException) => false
                case Failure(e: IllegalArgumentException) if String.valueOf(e.getMessage).contains("argument") => false
                case _ => true
            }
        }.mkString(",")
    }

    /** `"ok"` or `"name-mismatch:<companion name>"`. A private session name (`gbx__l1_*`) is allowed. */
    def install(spark: SparkSession, sqlName: String, fqn: String): String = {
        val c = companion(fqn)
        val privateName = "gbx__l1_" + c.name.stripPrefix("gbx_")
        if (sqlName != c.name && sqlName != privateName) {
            s"name-mismatch:${c.name}"
        } else {
            spark.sessionState.functionRegistry.registerFunction(FunctionIdentifier(sqlName), c.info(), c.builder())
            "ok"
        }
    }

    /** Install the GDAL spec engine expression of `sqlName` ([[GdalSpecExpr]]; its spec read from the JAR's copy of
      * the manifest, never from the caller) under `name`: `sqlName`, or its private `gbx__l1_<x>`. The builder takes
      * `min_args` to all of the manifest's arguments and fills the rest from the manifest's SQL defaults (parsed
      * once, here, by the session's parser), then the session's configuration ([[ExpressionConfigExpr]]). When every
      * argument but the input is a literal it is bound once, at analysis, so a value the function refuses fails the
      * query before it runs. `"ok"`, or why not. */
    def installSpec(spark: SparkSession, name: String, sqlName: String): String = {
        val fn = Try(GdalSpecs.function(sqlName)) match {
            case scala.util.Success(f) => f
            case Failure(e)            => return s"no-spec:${e.getMessage}"
        }
        if (name != sqlName && name != "gbx__l1_" + sqlName.stripPrefix("gbx_")) return s"name-mismatch:$sqlName"
        val defaults = fn.params.map(_.default.map(spark.sessionState.sqlParser.parseExpression))
        val builder: FunctionBuilder = (args: Seq[Expression]) => {
            if (args.length < fn.minArgs || args.length > fn.params.length) {
                throw new IllegalArgumentException(
                  s"$sqlName takes ${fn.minArgs} to ${fn.params.length} arguments (${fn.params.map(_.name).mkString(", ")}); " +
                      s"got ${args.length}"
                )
            }
            val all = args ++ defaults.drop(args.length).map(_.get)
            bindLiterals(spark, fn, all)
            GdalSpecExpr(sqlName, all :+ ExpressionConfigExpr())
        }
        val usage = s"$name(${fn.params.map(_.name).mkString(", ")}) - GDAL '${fn.gdal.alg}' on the GDAL spec engine."
        val info = new ExpressionInfo(classOf[GdalSpecExpr].getName, null, name, usage, "", "", "", "", "0.6.0", "", "built-in")
        spark.sessionState.functionRegistry.registerFunction(FunctionIdentifier(name), info, builder)
        "ok"
    }

    private def literal(v: Any): Any = v match {
        case s: UTF8String => s.toString
        case d: Decimal    => d.toDouble
        case other         => other
    }

    /** Bind the literal arguments of a one-raster spec on placeholder input props: the binders' refusals (an unknown
      * choice, a half-given pair) are raised now. A data error (a CRS GDAL cannot parse) stays one: an error tile per
      * row. GDAL is initialized from the session's configuration, as the expression initializes it on an executor. */
    private def bindLiterals(spark: SparkSession, fn: SpecFunction, all: Seq[Expression]): Unit =
        if (fn.gdal.inputs.head._2 == "raster" && all.tail.forall(_.foldable)) {
            GDALManager.init(ExpressionConfig(spark))
            val values = fn.params.tail.zip(all.tail).map { case (p, e) => p.name -> literal(e.eval()) }.toMap
            try GdalSpecArgv.build(fn, values, Seq(RasterProps(1, 1, 1, "Float32")))
            catch { case _: SpecDataException => () }
        }

    /** Install a Catalyst generator as a TABLE function (`FROM t, LATERAL f(...)`), the shape light UDTFs and UC
      * table functions share, and in the function registry (the select-list form) too. It is the wrap Spark uses
      * for its own generator table functions. `"ok"` or `"name-mismatch:<companion name>"`. */
    def installGenerator(spark: SparkSession, sqlName: String, fqn: String): String = {
        val c = companion(fqn)
        if (sqlName != c.name) {
            s"name-mismatch:${c.name}"
        } else {
            val b = c.builder()
            spark.sessionState.tableFunctionRegistry.registerFunction(
              FunctionIdentifier(sqlName),
              c.info(),
              (args: Seq[Expression]) => Generate(b(args).asInstanceOf[Generator], Nil, outer = false, None, Nil, OneRowRelation())
            )
            spark.sessionState.functionRegistry.registerFunction(FunctionIdentifier(sqlName), c.info(), b)
            "ok"
        }
    }
}
