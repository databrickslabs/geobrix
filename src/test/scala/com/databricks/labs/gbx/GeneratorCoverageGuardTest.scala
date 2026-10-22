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

    /** Registers every gbx package so the guard sees the complete set of heavyweight functions. */
    private def registerAll(): Unit = {
        gridx.bng.functions.register(spark)
        gridx.custom.functions.register(spark)
        gridx.quadbin.functions.register(spark)
        gridx.h3.functions.register(spark)
        rasterx.functions.register(spark)
        vectorx.functions.register(spark)
        vectorx.jts.legacy.functions.register(spark)
        pmtiles.functions.register(spark)
    }

    /** (SQL name, expression class name) of every registered gbx_* function. */
    private def registeredGbx(): Seq[(String, String)] = {
        registerAll()
        val registry = spark.sessionState.functionRegistry
        registry
            .listFunction()
            .map(_.funcName)
            .filter(_.startsWith("gbx_"))
            .sorted
            .map { name =>
                val info = registry.lookupFunction(org.apache.spark.sql.catalyst.FunctionIdentifier(name))
                (name, info.map(_.getClassName).getOrElse(""))
            }
    }

    test("every registered gbx function names an expression class that loads") {
        val registered = registeredGbx()
        assert(registered.size > 100, s"only ${registered.size} gbx_* functions registered: the guard would be vacuous")
        // A function whose class does not resolve cannot be classified, so it must not be dropped silently.
        val unloadable = registered.filter { case (_, className) => Try(Class.forName(className)).isFailure }
        assert(unloadable.isEmpty, s"registered functions whose ExpressionInfo class does not load: ${unloadable.mkString(", ")}")
    }

    test("every registered generator has a contract test, and every contract entry is registered") {
        val generators = registeredGbx().collect {
            case (name, className) if classOf[CollectionGenerator].isAssignableFrom(Class.forName(className)) => name
        }.toSet
        assert(generators.nonEmpty, "no generators found: the guard would pass vacuously")
        val covered = GeneratorContract.coverage.keySet
        assert(
          generators == covered,
          s"generators without a contract test: ${(generators -- covered).toSeq.sorted.mkString(", ")}; " +
              s"contract entries that are not registered generators: ${(covered -- generators).toSeq.sorted.mkString(", ")}. " +
              "Add a contract test and list the generator in GeneratorContract.coverage."
        )
    }

}
