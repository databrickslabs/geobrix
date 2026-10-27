package com.databricks.labs.gbx

import org.scalatest.exceptions.TestFailedException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** `DocExampleSource` finds the example of one function, and only of that function. */
class DocExampleSourceTest extends AnyFunSuite {

    private val q = "\"\"\""

    private val source =
        s"""def first_sql_example():
           |    return $q
           |-- first
           |SELECT 1;
           |$q
           |
           |def noreturn_sql_example():
           |    sql = "SELECT 2"
           |    return sql
           |
           |def third_sql_example():
           |    return $q
           |SELECT 3;
           |$q
           |
           |def unclosed_sql_example():
           |    return $q
           |SELECT 4;
           |""".stripMargin

    test("example is the string the named function returns, comments included") {
        DocExampleSource.exampleFrom(source, "f.py", "first").trim shouldBe "-- first\nSELECT 1;"
    }

    test("example of the last function of a file, which no def follows") {
        val last = s"def only_sql_example():\n    return ${q}SELECT 5;${q}\n"
        DocExampleSource.exampleFrom(last, "f.py", "only") shouldBe "SELECT 5;"
    }

    test("a function with no return does not take the example of the function after it") {
        val e = intercept[TestFailedException](DocExampleSource.exampleFrom(source, "f.py", "noreturn"))
        e.getMessage should (include("noreturn_sql_example") and include("f.py") and include("return"))
    }

    test("an unknown function fails naming it and the file") {
        val e = intercept[TestFailedException](DocExampleSource.exampleFrom(source, "f.py", "missing"))
        e.getMessage should (include("missing_sql_example") and include("f.py"))
    }

    test("a returned string that is never closed fails naming the function") {
        val e = intercept[TestFailedException](DocExampleSource.exampleFrom(source, "f.py", "unclosed"))
        e.getMessage should (include("unclosed_sql_example") and include("not closed"))
    }

    test("the docs' own examples are found by name") {
        DocExampleSource.sqlOf(DocExampleSource.example("gridx_functions_sql.py", "quadbin_geomkringexplode")) should include("gbx_quadbin_geomkringexplode")
    }

}
