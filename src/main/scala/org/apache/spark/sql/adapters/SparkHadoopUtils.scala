package org.apache.spark.sql.adapters

import org.apache.spark.deploy.SparkHadoopUtil

/** Adapter for SparkHadoopUtil (e.g. for compatibility across Spark versions). */
object SparkHadoopUtils {

    /** Returns the singleton SparkHadoopUtil instance. */
    def sdu: SparkHadoopUtil = SparkHadoopUtil.get

}
