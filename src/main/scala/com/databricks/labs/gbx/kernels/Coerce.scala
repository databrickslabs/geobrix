package com.databricks.labs.gbx.kernels

import com.databricks.labs.gbx.gridx.grid.BNG

import scala.util.Try

/** Java UDFs and UC handlers receive SQL values as boxed objects whose type depends on the literal or
  * column: coerce them here. A value that is not usable is a PARAMETER error and raises, naming the value;
  * only data errors (NaN, out of range coordinates) become NULL, and callers do that themselves. */
object Coerce {
    def double(v: Any): Double = v match {
        case n: java.lang.Number => n.doubleValue
        case other               => throw new IllegalArgumentException(s"expected a number, got ${other.getClass.getName}: $other")
    }

    /** A BNG resolution: an INT index or a resolution-map STRING, validated by [[BNG.getResolution]].
      * A UC STRING parameter receives an INT argument as its digits, so `digitStrings` also accepts those. */
    def bngResolution(v: Any, digitStrings: Boolean): Int = v match {
        case i: java.lang.Integer => BNG.getResolution(i.intValue)
        case l: java.lang.Long    => Try(Math.toIntExact(l)).toOption.map(BNG.getResolution(_)).getOrElse(unsupported(v))
        case s: String if digitStrings && s.matches("-?[0-9]+") => asInt(s).map(BNG.getResolution(_)).getOrElse(unsupported(v))
        case s: String            => BNG.getResolution(s)
        case other                => throw new IllegalArgumentException(
          s"BNG resolution must be INT or STRING, got ${other.getClass.getName}: $other")
    }

    private def asInt(digits: String): Option[Int] = Try(digits.toInt).toOption

    private def unsupported(v: Any): Nothing = throw new IllegalStateException(s"BNG resolution not supported; found $v")
}
