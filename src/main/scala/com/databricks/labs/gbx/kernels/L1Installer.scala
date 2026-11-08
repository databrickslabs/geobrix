package com.databricks.labs.gbx.kernels

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, OneRowRelation}

import scala.util.{Failure, Try}

/** The manager's L1 primitive: install ONE companion under the name the manifest gives.
  * Scala decides nothing here; the Python registrar says which names exist and at which level. */
object L1Installer {

    /** Spark 4.3 keys the function registries by a 3-part identifier and asserts on any other; a session function
      * is `system.session.<name>` there (what `createOrReplaceTempFunction` builds). Spark 4.0 to 4.2 key a session
      * function by its bare name, and a `session` database would make it a different key. The one JAR runs on both,
      * so the identifier is chosen at run time: `lookupFunctionEntry` exists on a registry from 4.3 on. */
    private val qualifiedRegistryIds: Boolean = registryIsQualified(classOf[org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase[_]])

    /** True when a registry class keys its functions by the 3-part identifier (it has `lookupFunctionEntry`). */
    private[kernels] def registryIsQualified(registryClass: Class[_]): Boolean =
        registryClass.getMethods.exists(_.getName == "lookupFunctionEntry")

    /** The identifier of a session function `name` for a registry that is `qualified` or not. */
    private[kernels] def sessionFunctionId(name: String, qualified: Boolean): FunctionIdentifier =
        if (qualified) new FunctionIdentifier(name, Some("session"), Some("system"))
        else FunctionIdentifier(name)

    /** The identifier a session function `name` is registered and looked up under, on the running Spark. */
    def sessionFunctionId(name: String): FunctionIdentifier = sessionFunctionId(name, qualifiedRegistryIds)

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
            spark.sessionState.functionRegistry.registerFunction(sessionFunctionId(sqlName), c.info(), c.builder())
            "ok"
        }
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
              sessionFunctionId(sqlName),
              c.info(),
              (args: Seq[Expression]) => Generate(b(args).asInstanceOf[Generator], Nil, outer = false, None, Nil, OneRowRelation())
            )
            spark.sessionState.functionRegistry.registerFunction(sessionFunctionId(sqlName), c.info(), b)
            "ok"
        }
    }
}
