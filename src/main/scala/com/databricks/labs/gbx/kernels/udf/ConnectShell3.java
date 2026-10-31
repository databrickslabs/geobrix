package com.databricks.labs.gbx.kernels.udf;

import org.apache.spark.sql.api.java.UDF3;

/**
 * The body of a three-argument Java shell: each value goes to the Scala implementation as SQL bound it, and the Scala
 * class coerces it (an INT resolution, INT or BIGINT coordinates). The shell's own class still names its concrete
 * parameter types in its {@code implements UDF3<...>} clause: Spark reads that clause from the class itself when the
 * shell is registered as a Java UDF, and standard access mode converts each input to those types (an {@code Object}
 * parameter there fails in the input conversion). Declaring {@code call} once here, on type variables, compiles to
 * {@code call(Object, Object, Object)} with no cast, so a session that passes values unconverted (a classic or a
 * Spark Connect session on a dedicated cluster) does not fail with a ClassCastException.
 */
public abstract class ConnectShell3<A, B, C> implements UDF3<A, B, C, String> {
    /** The Scala implementation (see {@link ConnectDelegate}). */
    protected abstract UDF3<Object, Object, Object, String> delegate();

    @Override
    public final String call(A a, B b, C c) throws Exception {
        return delegate().call(a, b, c);
    }
}
