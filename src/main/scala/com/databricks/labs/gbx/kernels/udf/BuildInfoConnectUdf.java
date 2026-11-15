package com.databricks.labs.gbx.kernels.udf;

import org.apache.spark.sql.api.java.UDF0;

/** {@link BuildInfoUdf} for standard access mode (a Java class: the manager reads the JAR identity through it). */
public class BuildInfoConnectUdf implements UDF0<String> {
    private static final UDF0<String> DELEGATE =
        ConnectDelegate.of("com.databricks.labs.gbx.kernels.udf.BuildInfoUdf");

    @Override
    public String call() throws Exception {
        return DELEGATE.call();
    }
}
