package com.databricks.labs.gbx.kernels

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.scalatest.funsuite.AnyFunSuite

/** The identifier an L1 session function is registered under is chosen at run time from the registry class
  * (`lookupFunctionEntry` exists from Spark 4.3): a bare name before, `system.session.<name>` from then on. */
class L1InstallerIdTest extends AnyFunSuite {

    /** A registry as Spark 4.3 has it: it can look a function entry up by its qualified identifier. */
    class QualifiedRegistryStub { def lookupFunctionEntry(id: FunctionIdentifier): Option[String] = None }

    /** A registry as Spark 4.0 to 4.2 have it. */
    class BareRegistryStub { def lookupFunction(id: FunctionIdentifier): Option[String] = None }

    test("a registry without lookupFunctionEntry takes the bare name") {
        assert(!L1Installer.registryIsQualified(classOf[BareRegistryStub]))
        val id = L1Installer.sessionFunctionId("gbx_rst_width", L1Installer.registryIsQualified(classOf[BareRegistryStub]))
        assert(id == FunctionIdentifier("gbx_rst_width"))
        assert(id.database.isEmpty && id.catalog.isEmpty)
    }

    test("a registry with lookupFunctionEntry takes the 3-part system.session identifier") {
        assert(L1Installer.registryIsQualified(classOf[QualifiedRegistryStub]))
        val id = L1Installer.sessionFunctionId("gbx_rst_width", L1Installer.registryIsQualified(classOf[QualifiedRegistryStub]))
        assert(id.funcName == "gbx_rst_width" && id.database.contains("session") && id.catalog.contains("system"))
    }

    test("the running Spark's choice follows its own registry class") {
        val real = classOf[org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase[_]]
        val id = L1Installer.sessionFunctionId("gbx_rst_width")
        if (real.getMethods.exists(_.getName == "lookupFunctionEntry")) assert(id.catalog.contains("system"))
        else assert(id == FunctionIdentifier("gbx_rst_width"))
    }
}
