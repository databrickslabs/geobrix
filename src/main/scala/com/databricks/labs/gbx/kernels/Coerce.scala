package com.databricks.labs.gbx.kernels

/** Java UDFs receive SQL values as boxed objects whose type depends on the literal or column: coerce numbers here. */
object Coerce {
    def double(v: Any): Double = v match {
        case n: java.lang.Number => n.doubleValue
        case other               => throw new IllegalArgumentException(s"expected a number, got ${other.getClass.getName}")
    }
}
