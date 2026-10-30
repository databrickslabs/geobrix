package com.databricks.labs.gbx.kernels.udf;

/**
 * Hands a Java handler its Scala implementation. Standard access mode loads only Java classes as Java UDFs, with
 * concrete parameter types, so each handler of this package that ends in {@code ConnectUdf} is a thin Java shell
 * over the Scala class that holds the logic. javac compiles Java before scalac in this build, so the Scala class
 * is looked up by name, once, when the shell loads; every row calls it through the UDF interface.
 */
final class ConnectDelegate {
    private ConnectDelegate() {}

    @SuppressWarnings("unchecked")
    static <T> T of(String scalaClass) {
        try {
            return (T) Class.forName(scalaClass, true, ConnectDelegate.class.getClassLoader())
                .getDeclaredConstructor()
                .newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot load " + scalaClass, e);
        }
    }
}
