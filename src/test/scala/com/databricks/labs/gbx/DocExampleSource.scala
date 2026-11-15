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
        exampleFrom(src, file, fn)
    }

    /**
      * As [[example]], from the text of the doc-test source `file`. The `return """` is searched for in the body of
      * `<fn>_sql_example` only, which ends at the next top-level `def`: a function without one cannot hand back the
      * example of the function after it. A missing function, return or closing quotes fails with a message naming them.
      */
    private[gbx] def exampleFrom(src: String, file: String, fn: String): String = {
        val header = s"def ${fn}_sql_example():"
        val start = src.indexOf(header)
        assert(start >= 0, s"no ${fn}_sql_example in $file")
        val end = src.indexOf("\ndef ", start + header.length) match {
            case -1 => src.length
            case i  => i
        }
        val body = src.substring(start, end)
        val marker = "return \"\"\""
        val ret = body.indexOf(marker)
        assert(ret >= 0, s"${fn}_sql_example in $file has no `$marker` in its body")
        val open = ret + marker.length
        val close = body.indexOf("\"\"\"", open)
        assert(close >= 0, s"the string ${fn}_sql_example returns in $file is not closed")
        body.substring(open, close)
    }

    /** The runnable SQL of an example: comment lines dropped, trailing `;` removed. */
    def sqlOf(example: String): String =
        example.linesIterator.map(_.trim).filterNot(l => l.isEmpty || l.startsWith("--")).mkString(" ").stripSuffix(";")

}
