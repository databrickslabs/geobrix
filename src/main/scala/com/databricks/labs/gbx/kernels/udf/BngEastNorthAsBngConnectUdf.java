package com.databricks.labs.gbx.kernels.udf;

import org.apache.spark.sql.api.java.UDF3;

/**
 * gbx_bng_eastnorthasbng for standard access mode: the Java shell over {@link BngEastNorthAsBngUdf}, which coerces
 * each value (an INT resolution is read as its index, INT coordinates as doubles). Its concrete parameter types are
 * the ones standard access mode converts the inputs to (see {@link ConnectShell3}).
 */
public class BngEastNorthAsBngConnectUdf extends ConnectShell3<Double, Double, String>
        implements UDF3<Double, Double, String, String> {
    private static final UDF3<Object, Object, Object, String> DELEGATE =
        ConnectDelegate.of("com.databricks.labs.gbx.kernels.udf.BngEastNorthAsBngUdf");

    @Override
    protected UDF3<Object, Object, Object, String> delegate() {
        return DELEGATE;
    }
}
