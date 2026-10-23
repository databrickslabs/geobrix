package com.databricks.labs.gbx

import org.scalatest.Assertions._

/** Reads the SQL examples the docs publish, from their doc-test source, so a test runs the text users see. */
object DocExampleSource {

    /** The string returned by `def <fn>_sql_example()` in `docs/tests/python/api/<file>`, as written (comments included). */
    def example(file: String, fn: String): String = {
        val path = new java.io.File(sys.props.getOrElse("basedir", "."), s"docs/tests/python/api/$file")
        assert(path.exists, s"docs example source not found: ${path.getAbsolutePath}")
        val source = scala.io.Source.fromFile(path, "UTF-8")
        val src = try source.mkString finally source.close()
        val start = src.indexOf(s"def ${fn}_sql_example():")
        assert(start >= 0, s"no ${fn}_sql_example in $file")
        val open = src.indexOf("return \"\"\"", start) + "return \"\"\"".length
        src.substring(open, src.indexOf("\"\"\"", open))
    }

    /** The runnable SQL of an example: comment lines dropped, trailing `;` removed. */
    def sqlOf(example: String): String =
        example.linesIterator.map(_.trim).filterNot(l => l.isEmpty || l.startsWith("--")).mkString(" ").stripSuffix(";")

}
