package com.databricks.labs.gbx.kernels

import com.databricks.labs.gbx.gridx.grid.BNG

import scala.util.Try

/** Java UDFs and UC handlers receive SQL values as boxed objects whose type depends on the literal or
  * column: coerce them here. A value that is not usable is a PARAMETER error and raises, naming the value;
  * only data errors (NaN, out of range coordinates) become NULL, and callers do that themselves.
  *
  * These facades run in sandboxes whose classpath is little more than the JAR and `spark-sql-api`, so nothing
  * here may reach `UTF8String` or any other Spark runtime class: `BNG.getResolution(Any)` does (its pattern
  * match casts to `UTF8String`), which is why the resolution is validated against `BNG.resolutions` and
  * `BNG.resolutionMap` directly. */
object Coerce {
    def double(v: Any): Double = v match {
        case n: java.lang.Number => n.doubleValue
        case other               => throw new IllegalArgumentException(s"expected a number, got ${other.getClass.getName}: $other")
    }

    /** A BNG resolution: an INT index or a resolution-map STRING; anything else raises `found <value>`.
      * A UC STRING parameter receives an INT argument as its digits, so `digitStrings` also accepts those. */
    def bngResolution(v: Any, digitStrings: Boolean): Int = v match {
        case i: java.lang.Integer => index(i.intValue, v)
        case l: java.lang.Long    => Try(Math.toIntExact(l)).toOption.map(index(_, v)).getOrElse(unsupported(v))
        case s: String if digitStrings && s.matches("-?[0-9]+") => Try(s.toInt).toOption.map(index(_, v)).getOrElse(unsupported(v))
        case s: String            => BNG.resolutionMap.getOrElse(s, unsupported(v))
        case other                => throw new IllegalArgumentException(
          s"BNG resolution must be INT or STRING, got ${other.getClass.getName}: $other")
    }

    private def index(i: Int, original: Any): Int = if (BNG.resolutions.contains(i)) i else unsupported(original)

    private def unsupported(v: Any): Nothing = throw new IllegalStateException(s"BNG resolution not supported; found $v")
}
