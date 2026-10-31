package com.databricks.labs.gbx.kernels.udf;

import org.apache.spark.sql.api.java.UDF3;

/**
 * gbx_bng_eastnorthasbng for standard access mode: the typed Java shell over {@link BngEastNorthAsBngUdf}. The
 * resolution arrives as a string (an INT is cast to its digits, which the Scala class reads as an index).
 */
public class BngEastNorthAsBngConnectUdf implements UDF3<Double, Double, String, String> {
    private static final UDF3<Object, Object, Object, String> DELEGATE =
        ConnectDelegate.of("com.databricks.labs.gbx.kernels.udf.BngEastNorthAsBngUdf");

    @Override
    public String call(Double easting, Double northing, String resolution) throws Exception {
        return DELEGATE.call(easting, northing, resolution);
    }
}
