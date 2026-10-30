package com.databricks.labs.gbx.kernels.udf;

import org.apache.spark.sql.api.java.UDF0;

/** {@link GdalProbeUdf} for standard access mode (a Java class: the manager learns through it whether GDAL loads). */
public class GdalProbeConnectUdf implements UDF0<String> {
    private static final UDF0<String> DELEGATE =
        ConnectDelegate.of("com.databricks.labs.gbx.kernels.udf.GdalProbeUdf");

    @Override
    public String call() throws Exception {
        return DELEGATE.call();
    }
}
