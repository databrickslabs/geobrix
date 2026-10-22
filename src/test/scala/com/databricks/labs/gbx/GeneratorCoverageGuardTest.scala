package com.databricks.labs.gbx

import org.apache.spark.sql.catalyst.expressions.CollectionGenerator
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession

import scala.util.Try

/**
  * Durable guard for the generator contract: the heavyweight generators registered in a session must be
  * exactly the ones [[GeneratorContract.coverage]] lists, and every listed generator has a contract test.
  * A generator added (or removed) without updating the contract fails here.
  */
class GeneratorCoverageGuardTest extends PlanTest with SilentSparkSession {

    /** SQL names of every registered gbx_* function whose expression is a CollectionGenerator. */
    private def registeredGenerators(): Set[String] = {
        gridx.bng.functions.register(spark)
        gridx.custom.functions.register(spark)
        gridx.quadbin.functions.register(spark)
        gridx.h3.functions.register(spark)
        rasterx.functions.register(spark)
        vectorx.functions.register(spark)
        val registry = spark.sessionState.functionRegistry
        registry
            .listFunction()
            .map(_.funcName)
            .filter(_.startsWith("gbx_"))
            .filter { name =>
                registry
                    .lookupFunction(org.apache.spark.sql.catalyst.FunctionIdentifier(name))
                    .exists(info => Try(Class.forName(info.getClassName)).toOption.exists(classOf[CollectionGenerator].isAssignableFrom))
            }
            .toSet
    }

    test("every registered generator has a contract test, and every contract entry is registered") {
        val registered = registeredGenerators()
        assert(registered.nonEmpty, "no generators found: the guard would pass vacuously")
        val covered = GeneratorContract.coverage.keySet
        assert(
          registered == covered,
          s"generators without a contract test: ${(registered -- covered).toSeq.sorted.mkString(", ")}; " +
              s"contract entries that are not registered generators: ${(covered -- registered).toSeq.sorted.mkString(", ")}. " +
              "Add a contract test and list the generator in GeneratorContract.coverage."
        )
    }

}
